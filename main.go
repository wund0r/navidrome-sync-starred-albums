package main

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	apiVersion = "1.16.1"
	clientID   = "navidrome-starred-albums-sync"
)

type List[T any] []T

func (l *List[T]) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "" || s == "null" {
		*l = nil
		return nil
	}

	if strings.HasPrefix(s, "[") {
		var v []T
		if err := json.Unmarshal(b, &v); err != nil {
			return err
		}
		*l = v
		return nil
	}

	var one T
	if err := json.Unmarshal(b, &one); err != nil {
		return err
	}

	*l = []T{one}
	return nil
}

type envelope struct {
	Response subsonicResponse `json:"subsonic-response"`
}

type subsonicResponse struct {
	Status  string          `json:"status"`
	Version string          `json:"version"`
	Error   *subsonicError  `json:"error,omitempty"`
	Starred *starredPayload `json:"starred2,omitempty"`
	Album   *albumPayload   `json:"album,omitempty"`
}

type subsonicError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type starredPayload struct {
	Albums List[Album] `json:"album"`
}

type Album struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Artist    string `json:"artist"`
	SongCount int    `json:"songCount"`
}

type albumPayload struct {
	ID     string     `json:"id"`
	Name   string     `json:"name"`
	Artist string     `json:"artist"`
	Songs  List[Song] `json:"song"`
}

type Song struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Album       string `json:"album"`
	Artist      string `json:"artist"`
	Track       int    `json:"track"`
	DiscNumber  int    `json:"discNumber"`
	Path        string `json:"path"`
	Suffix      string `json:"suffix"`
	Size        int64  `json:"size"`
	ContentType string `json:"contentType"`
}

type Config struct {
	Server    string
	User      string
	Password  string
	Dest      string
	Workers   int
	Overwrite bool
	DryRun    bool
	Prune     bool
	Timeout   time.Duration
}

type Client struct {
	baseURL  string
	user     string
	password string
	http     *http.Client
}

type Job struct {
	Album Album
	Song  Song
	Path  string
}

func main() {
	cfg := parseFlags()

	ctx := context.Background()

	client := &Client{
		baseURL:  strings.TrimRight(cfg.Server, "/"),
		user:     cfg.User,
		password: cfg.Password,
		http:     &http.Client{Timeout: cfg.Timeout},
	}

	fmt.Printf("Connecting to %s\n", cfg.Server)

	albums, err := client.GetStarredAlbums(ctx)
	fatalIf(err)

	fmt.Printf("Found %d starred album(s)\n", len(albums))

	jobs := make(chan Job)

	var wg sync.WaitGroup
	var mu sync.Mutex

	var downloaded int
	var skipped int
	var failed int
	var pruned int

	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for job := range jobs {
				if cfg.DryRun {
					fmt.Printf("[dry-run] get   %s\n", job.Path)

					mu.Lock()
					skipped++
					mu.Unlock()

					continue
				}

				result, err := client.DownloadSong(ctx, job.Song, job.Path, cfg.Overwrite)

				mu.Lock()
				switch {
				case err != nil:
					failed++
					fmt.Fprintf(os.Stderr, "ERROR: %s - %v\n", job.Path, err)
				case result == "skipped":
					skipped++
					fmt.Printf("skip  %s\n", job.Path)
				default:
					downloaded++
					fmt.Printf("get   %s\n", job.Path)
				}
				mu.Unlock()
			}
		}()
	}

	starredAlbumDirs := map[string]bool{}

	for _, album := range albums {
		fullAlbum, err := client.GetAlbum(ctx, album.ID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: album %q by %q: %v\n", album.Name, album.Artist, err)

			mu.Lock()
			failed++
			mu.Unlock()

			continue
		}

		albumDirsForThisAlbum := map[string]bool{}

		for _, song := range fullAlbum.Songs {
			rel, err := relativeSongPath(song)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: song %q: %v\n", song.Title, err)

				mu.Lock()
				failed++
				mu.Unlock()

				continue
			}

			albumDir, err := albumDirectoryFromRelativeSongPath(rel)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: album %q by %q: %v\n", album.Name, album.Artist, err)

				mu.Lock()
				failed++
				mu.Unlock()

				continue
			}

			starredAlbumDirs[albumDir] = true
			albumDirsForThisAlbum[albumDir] = true

			target := filepath.Join(cfg.Dest, rel)
			jobs <- Job{Album: album, Song: song, Path: target}
		}

		if len(albumDirsForThisAlbum) > 1 {
			fmt.Fprintf(
				os.Stderr,
				"WARNING: album %q by %q spans multiple directories: %v\n",
				album.Name,
				album.Artist,
				mapKeys(albumDirsForThisAlbum),
			)
		}
	}

	close(jobs)
	wg.Wait()

	if cfg.Prune {
		removed, pruneFailures := pruneUnstarredAlbumDirs(cfg.Dest, starredAlbumDirs, cfg.DryRun)
		pruned = removed
		failed += pruneFailures
	}

	fmt.Printf("\nDone. downloaded=%d skipped=%d pruned=%d failed=%d\n", downloaded, skipped, pruned, failed)

	if failed > 0 {
		os.Exit(1)
	}
}

func parseFlags() Config {
	server := flag.String("server", getenv("NAVIDROME_URL", ""), "Navidrome base URL, e.g. https://music.example.com")
	user := flag.String("user", getenv("NAVIDROME_USER", ""), "Navidrome username")
	pass := flag.String("pass", getenv("NAVIDROME_PASS", ""), "Navidrome password")
	dest := flag.String("dest", getenv("NAVIDROME_DEST", ""), "Destination directory")
	workers := flag.Int("workers", 4, "Concurrent downloads")
	overwrite := flag.Bool("overwrite", false, "Overwrite existing files")
	dryRun := flag.Bool("dry-run", false, "Print actions without writing or deleting files")
	prune := flag.Bool("prune", false, "Remove destination album directories that are not currently starred")
	timeout := flag.Duration("timeout", 120*time.Second, "HTTP timeout per request")

	flag.Parse()

	if *server == "" || *user == "" || *pass == "" || *dest == "" {
		fmt.Fprintln(os.Stderr, "Required: -server, -user, -pass, -dest")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Example:")
		fmt.Fprintln(os.Stderr, `  go run navidrome-starred-albums-sync.go -server https://music.example.com -user me -pass 'secret' -dest /backup/music -prune`)
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Or set NAVIDROME_URL, NAVIDROME_USER, NAVIDROME_PASS, NAVIDROME_DEST.")
		os.Exit(2)
	}

	if *workers < 1 {
		*workers = 1
	}

	return Config{
		Server:    *server,
		User:      *user,
		Password:  *pass,
		Dest:      *dest,
		Workers:   *workers,
		Overwrite: *overwrite,
		DryRun:    *dryRun,
		Prune:     *prune,
		Timeout:   *timeout,
	}
}

func (c *Client) GetStarredAlbums(ctx context.Context) ([]Album, error) {
	var out envelope

	if err := c.getJSON(ctx, "getStarred2", nil, &out); err != nil {
		return nil, err
	}

	if out.Response.Starred == nil {
		return nil, nil
	}

	return out.Response.Starred.Albums, nil
}

func (c *Client) GetAlbum(ctx context.Context, albumID string) (*albumPayload, error) {
	params := url.Values{}
	params.Set("id", albumID)

	var out envelope

	if err := c.getJSON(ctx, "getAlbum", params, &out); err != nil {
		return nil, err
	}

	if out.Response.Album == nil {
		return nil, fmt.Errorf("empty album response for id %s", albumID)
	}

	return out.Response.Album, nil
}

func (c *Client) DownloadSong(ctx context.Context, song Song, target string, overwrite bool) (string, error) {
	if !overwrite {
		if st, err := os.Stat(target); err == nil {
			if song.Size <= 0 || st.Size() == song.Size {
				return "skipped", nil
			}
		}
	}

	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return "", err
	}

	params := url.Values{}
	params.Set("id", song.ID)

	reqURL := c.endpoint("download", params)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return "", err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("download failed: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	if looksLikeAPIError(resp.Header.Get("Content-Type")) {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return "", fmt.Errorf("download returned API error instead of media: %s", strings.TrimSpace(string(body)))
	}

	tmp := target + ".part"

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return "", err
	}

	n, copyErr := io.Copy(f, resp.Body)
	closeErr := f.Close()

	if copyErr != nil {
		_ = os.Remove(tmp)
		return "", copyErr
	}

	if closeErr != nil {
		_ = os.Remove(tmp)
		return "", closeErr
	}

	if song.Size > 0 && n != song.Size {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("size mismatch: got %d bytes, expected %d", n, song.Size)
	}

	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}

	return "downloaded", nil
}

func (c *Client) getJSON(ctx context.Context, method string, params url.Values, dest any) error {
	if params == nil {
		params = url.Values{}
	}

	params.Set("f", "json")

	reqURL := c.endpoint(method, params)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	if err := json.Unmarshal(body, dest); err != nil {
		return fmt.Errorf("decode JSON: %w; body=%s", err, string(body))
	}

	if e, ok := dest.(*envelope); ok {
		if e.Response.Status == "failed" {
			if e.Response.Error != nil {
				return fmt.Errorf("subsonic error %d: %s", e.Response.Error.Code, e.Response.Error.Message)
			}

			return errors.New("subsonic error: status=failed")
		}
	}

	return nil
}

func (c *Client) endpoint(method string, params url.Values) string {
	if params == nil {
		params = url.Values{}
	}

	salt := randomSalt()
	token := md5Hex(c.password + salt)

	params.Set("u", c.user)
	params.Set("t", token)
	params.Set("s", salt)
	params.Set("v", apiVersion)
	params.Set("c", clientID)

	return c.baseURL + "/rest/" + method + ".view?" + params.Encode()
}

func relativeSongPath(song Song) (string, error) {
	p := strings.TrimSpace(song.Path)
	if p == "" {
		p = fallbackSongPath(song)
	}

	p = filepath.FromSlash(p)
	p = filepath.Clean(p)

	if filepath.IsAbs(p) || p == "." || p == ".." || strings.HasPrefix(p, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("unsafe path from server: %q", song.Path)
	}

	return p, nil
}

func albumDirectoryFromRelativeSongPath(rel string) (string, error) {
	rel = filepath.Clean(rel)

	if rel == "." || rel == "" || filepath.IsAbs(rel) {
		return "", fmt.Errorf("unsafe relative path: %q", rel)
	}

	parts := splitPath(rel)

	// This assumes the mirrored Navidrome path is:
	// Artist/Album/Track.ext
	if len(parts) < 3 {
		return "", fmt.Errorf("path does not contain artist/album/file structure: %q", rel)
	}

	return filepath.Join(parts[0], parts[1]), nil
}

func pruneUnstarredAlbumDirs(dest string, keep map[string]bool, dryRun bool) (removed int, failed int) {
	artistEntries, err := os.ReadDir(dest)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0
		}

		fmt.Fprintf(os.Stderr, "ERROR: reading destination %q: %v\n", dest, err)
		return 0, 1
	}

	for _, artistEntry := range artistEntries {
		if !artistEntry.IsDir() {
			continue
		}

		artistName := artistEntry.Name()
		artistPath := filepath.Join(dest, artistName)

		albumEntries, err := os.ReadDir(artistPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: reading artist directory %q: %v\n", artistPath, err)
			failed++
			continue
		}

		for _, albumEntry := range albumEntries {
			if !albumEntry.IsDir() {
				continue
			}

			albumRel := filepath.Join(artistName, albumEntry.Name())
			albumPath := filepath.Join(dest, albumRel)

			if keep[albumRel] {
				continue
			}

			if dryRun {
				fmt.Printf("[dry-run] prune %s\n", albumPath)
				removed++
				continue
			}

			if err := os.RemoveAll(albumPath); err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: pruning %q: %v\n", albumPath, err)
				failed++
				continue
			}

			fmt.Printf("prune %s\n", albumPath)
			removed++
		}

		removeEmptyDir(artistPath, dryRun)
	}

	return removed, failed
}

func removeEmptyDir(path string, dryRun bool) {
	entries, err := os.ReadDir(path)
	if err != nil || len(entries) > 0 {
		return
	}

	if dryRun {
		fmt.Printf("[dry-run] prune empty artist dir %s\n", path)
		return
	}

	if err := os.Remove(path); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: removing empty directory %q: %v\n", path, err)
	}
}

func splitPath(p string) []string {
	p = filepath.Clean(p)
	parts := strings.Split(p, string(os.PathSeparator))

	out := make([]string, 0, len(parts))

	for _, part := range parts {
		if part != "" && part != "." {
			out = append(out, part)
		}
	}

	return out
}

func fallbackSongPath(song Song) string {
	artist := cleanPathPart(firstNonEmpty(song.Artist, "Unknown Artist"))
	album := cleanPathPart(firstNonEmpty(song.Album, "Unknown Album"))
	title := cleanPathPart(firstNonEmpty(song.Title, song.ID))

	ext := strings.TrimPrefix(song.Suffix, ".")
	if ext == "" {
		ext = extensionFromContentType(song.ContentType)
	}
	if ext == "" {
		ext = "bin"
	}

	prefix := ""
	if song.Track > 0 {
		prefix = fmt.Sprintf("%02d - ", song.Track)
	}

	return filepath.Join(artist, album, prefix+title+"."+ext)
}

func cleanPathPart(s string) string {
	s = strings.TrimSpace(s)

	replacer := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		string(os.PathSeparator), "_",
	)

	s = replacer.Replace(s)

	if s == "" || s == "." || s == ".." {
		return "_"
	}

	return s
}

func extensionFromContentType(ct string) string {
	if ct == "" {
		return ""
	}

	mediaType, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return ""
	}

	switch mediaType {
	case "audio/flac":
		return "flac"
	case "audio/mpeg":
		return "mp3"
	case "audio/ogg":
		return "ogg"
	case "audio/mp4", "audio/x-m4a":
		return "m4a"
	case "audio/wav", "audio/x-wav":
		return "wav"
	default:
		return ""
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}

	return ""
}

func looksLikeAPIError(contentType string) bool {
	contentType = strings.ToLower(contentType)

	return strings.Contains(contentType, "application/json") ||
		strings.Contains(contentType, "text/xml") ||
		strings.Contains(contentType, "application/xml")
}

func randomSalt() string {
	var b [12]byte

	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}

	return hex.EncodeToString(b[:])
}

func md5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

func getenv(k string, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}

	return fallback
}

func fatalIf(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func mapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))

	for k := range m {
		keys = append(keys, k)
	}

	return keys
}
