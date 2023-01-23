package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/manifest"
	"github.com/distribution/distribution/v3/manifest/schema2"
	"github.com/distribution/distribution/v3/reference"
	"github.com/distribution/distribution/v3/registry/api/errcode"
	"github.com/distribution/distribution/v3/registry/client"
	"github.com/distribution/distribution/v3/registry/client/auth"
	"github.com/distribution/distribution/v3/registry/client/auth/challenge"
	"github.com/distribution/distribution/v3/registry/client/transport"
	"github.com/dmage/boater/pkg/httplog"
	"github.com/opencontainers/go-digest"
)

var (
	username = flag.String("username", "", "username")
	password = flag.String("password", "", "password")
)

type simpleCredentialStore struct {
	username string
	password string
}

func (s *simpleCredentialStore) Basic(*url.URL) (string, string) {
	return s.username, s.password
}

func (s *simpleCredentialStore) RefreshToken(*url.URL, string) string {
	return ""
}

func (s *simpleCredentialStore) SetRefreshToken(*url.URL, string, string) {
}

func ping(manager challenge.Manager, endpoint string) error {
	resp, err := http.Get(endpoint)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return manager.AddResponse(resp)
}

// if x is "", it returns "\x00"
// if x is "\x01", it returns "\x02"
// if x is "\xfe", it returns "\xff"
// if x is "\xff", it returns "\x00\x00"
// if x is "\x00\xff", it returns "\x01\x00"
// if x is "\xff\xff", it returns "\x00\x00\x00"
func inc(x []byte) []byte {
	if len(x) == 0 {
		return []byte{0}
	}
	if x[len(x)-1] != 0xff {
		c := make([]byte, len(x))
		copy(c, x)
		c[len(c)-1]++
		return c
	}
	return append(inc(x[:len(x)-1]), 0)
}

func blobContentGenerator() <-chan string {
	ch := make(chan string)
	buf := ""
	go func() {
		for {
			buf = string(inc([]byte(buf)))
			ch <- buf
		}
	}()
	return ch
}

type Repository struct {
	client     *http.Client
	url        string
	repoName   string
	repository distribution.Repository
}

func pushBlob(ctx context.Context, repo *Repository, contentType string, blobContent string) (distribution.Descriptor, error) {
	digest := digest.FromString(blobContent)
	endpoint := repo.url + "/v2/" + repo.repoName + "/blobs/uploads/?digest=" + digest.String()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(blobContent))
	if err != nil {
		return distribution.Descriptor{}, err
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(blobContent)))
	req.Header.Set("Content-Type", contentType)
	resp, err := repo.client.Do(req)
	if err != nil {
		return distribution.Descriptor{}, err
	}
	if resp.StatusCode == http.StatusCreated {
		return distribution.Descriptor{
			MediaType: contentType,
			Digest:    digest,
			Size:      int64(len(blobContent)),
		}, nil
	}
	if resp.StatusCode != http.StatusAccepted {
		return distribution.Descriptor{}, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	log.Printf("Atomic upload is not supported, falling back to chunked upload")
	location := resp.Header.Get("Location")
	if location == "" {
		return distribution.Descriptor{}, fmt.Errorf("Location header is empty")
	}
	u, err := url.Parse(location)
	if err != nil {
		return distribution.Descriptor{}, fmt.Errorf("failed to parse Location header: %w", err)
	}
	q := u.Query()
	q.Set("digest", digest.String())
	u.RawQuery = q.Encode()
	req, err = http.NewRequestWithContext(ctx, http.MethodPut, u.String(), strings.NewReader(blobContent))
	if err != nil {
		return distribution.Descriptor{}, err
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(blobContent)))
	req.Header.Set("Content-Type", contentType)
	resp, err = repo.client.Do(req)
	if err != nil {
		return distribution.Descriptor{}, err
	}
	if resp.StatusCode != http.StatusCreated {
		return distribution.Descriptor{}, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return distribution.Descriptor{
		MediaType: contentType,
		Digest:    digest,
		Size:      int64(len(blobContent)),
	}, nil
}

// runNWorkers run n workers that handle a range from begin to end-1 in parallel.
func runNWorkers(n int, begin, end int, job func(i int) error) error {
	ch := make(chan int)
	errCh := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range ch {
				err := job(i)
				if err != nil {
					errCh <- err
					break
				}
			}
			// if there is an error, drain the channel
			for range ch {
			}
		}()
	}
	for i := begin; i < end; i++ {
		ch <- i
	}
	close(ch)
	wg.Wait()
	close(errCh)
	return <-errCh
}

func addDetails(err error) error {
	switch e := err.(type) {
	case errcode.Errors:
		for i := range e {
			e[i] = addDetails(e[i])
		}
		return e
	case errcode.Error:
		if e.Detail != nil {
			return fmt.Errorf("%w (%+v)", e, e.Detail)
		}
		return err
	}
	return err
}

type RootFS struct {
	Type string `json:"type"`
}
type Config struct {
	History []struct{} `json:"history"`
	RootFS  RootFS     `json:"rootfs"`
}

func pushImage(ctx context.Context, repo *Repository, tag string, N int, contentSource <-chan string) error {
	config := Config{
		History: make([]struct{}, N),
		RootFS: RootFS{
			Type: "layers",
		},
	}

	configMediaType := "application/vnd.docker.container.image.v1+json"
	configContent, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	configDesc, err := pushBlob(ctx, repo, configMediaType, string(configContent))
	if err != nil {
		return fmt.Errorf("failed to push config: %w", err)
	}

	layers := make([]distribution.Descriptor, N)
	err = runNWorkers(20, 0, len(layers), func(i int) error {
		//log.Printf("pushing layer %d...", i)
		desc, err := pushBlob(ctx, repo, "application/vnd.docker.image.rootfs.diff.tar.gzip", <-contentSource)
		if err != nil {
			return err
		}
		layers[i] = desc
		return nil
	})
	if err != nil {
		return err
	}

	manifest, err := schema2.FromStruct(schema2.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     schema2.MediaTypeManifest,
		},
		Config: distribution.Descriptor{
			MediaType: configMediaType,
			Size:      int64(len(configContent)),
			Digest:    configDesc.Digest,
		},
		Layers: layers,
	})
	if err != nil {
		return err
	}

	ms, err := repo.repository.Manifests(ctx)
	if err != nil {
		return err
	}

	log.Printf("pushing manifest %s...", tag)
	_, err = ms.Put(ctx, manifest, distribution.WithTag(tag))
	if registryError, err := err.(errcode.Error); err {
		return fmt.Errorf("failed to push manifest: %w; details: %v", registryError, registryError.Detail)
	}

	return addDetails(err)
}

func pushConfig(ctx context.Context, repo *Repository, config Config, contentSource <-chan string) (distribution.Descriptor, error) {
	configMediaType := "application/vnd.docker.container.image.v1+json"
	configContent, err := json.Marshal(config)
	if err != nil {
		return distribution.Descriptor{}, fmt.Errorf("failed to marshal config: %w", err)
	}
	configDesc, err := pushBlob(ctx, repo, configMediaType, string(configContent))
	if err != nil {
		return distribution.Descriptor{}, fmt.Errorf("failed to push config: %w", err)
	}
	return configDesc, nil
}

type exponentiallyMovingAverage struct {
	mu    sync.Mutex
	alpha float64
	value time.Duration
}

func (a *exponentiallyMovingAverage) Add(value time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.value = time.Duration(float64(a.value)*a.alpha + float64(value)*(1-a.alpha))
}

func (a *exponentiallyMovingAverage) Value() time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.value
}

var avgPushTime = &exponentiallyMovingAverage{
	alpha: 0.99,
}

func pushDummyImage(ctx context.Context, repo *Repository, tag string, configDesc distribution.Descriptor, contentSource <-chan string) error {
	manifest, err := schema2.FromStruct(schema2.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     schema2.MediaTypeManifest,
		},
		Config: distribution.Descriptor{
			MediaType: configDesc.MediaType,
			Size:      configDesc.Size,
			Digest:    configDesc.Digest,
			Annotations: map[string]string{
				"x": base64.StdEncoding.EncodeToString([]byte(<-contentSource)),
			},
		},
		Layers: []distribution.Descriptor{
			{
				MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
				Size:      configDesc.Size,
				Digest:    configDesc.Digest,
			},
		},
	})
	if err != nil {
		return err
	}

	ms, err := repo.repository.Manifests(ctx)
	if err != nil {
		return err
	}

	begin := time.Now()
	_, err = ms.Put(ctx, manifest, distribution.WithTag(tag))
	end := time.Now()
	avgPushTime.Add(end.Sub(begin))
	if registryError, err := err.(errcode.Error); err {
		return fmt.Errorf("failed to push manifest: %w; details: %v", registryError, registryError.Detail)
	}
	log.Printf("[avg push time %6s] manifest %s pushed in %s", avgPushTime.Value().Truncate(time.Millisecond), tag, end.Sub(begin).Truncate(time.Millisecond))

	return addDetails(err)
}

func pushImagesIntoRepository(ctx context.Context, repoImageReference string, imagesPerRepository, blobsPerImage int, contentSource <-chan string) error {
	named, err := reference.ParseNormalizedNamed(repoImageReference)
	if err != nil {
		return err
	}

	url := "http://" + reference.Domain(named)

	repoName := reference.Path(named)
	path, err := reference.WithName(repoName)
	if err != nil {
		return err
	}

	credentialStore := &simpleCredentialStore{
		username: *username,
		password: *password,
	}

	challengeManager := challenge.NewSimpleManager()

	httpLogRT := &httplog.RoundTripper{
		N: 20 * 16,
	}
	_ = httpLogRT
	rt := transport.NewTransport(
		nil, // httpLogRT,
		auth.NewAuthorizer(
			challengeManager,
			auth.NewTokenHandler(nil, credentialStore, repoName, "pull", "push"),
		),
	)

	repository, err := client.NewRepository(path, url, rt)
	if err != nil {
		return err
	}

	err = ping(challengeManager, url+"/v2/")
	if err != nil {
		return err
	}

	repo := &Repository{
		client: &http.Client{
			Transport: rt,
		},
		url:        url,
		repoName:   repoName,
		repository: repository,
	}

	configDesc, err := pushConfig(ctx, repo, Config{
		History: make([]struct{}, 1),
		RootFS: RootFS{
			Type: "layers",
		},
	}, contentSource)
	if err != nil {
		return err
	}

	return runNWorkers(30, 0, imagesPerRepository, func(i int) error {
		//return pushImage(ctx, repo, fmt.Sprintf("random%08d", i), blobsPerImage, contentSource)
		return pushDummyImage(ctx, repo, fmt.Sprintf("random%08d", i), configDesc, contentSource)
	})
}

func main() {
	flag.Parse()
	ctx := context.Background()
	contentSource := blobContentGenerator()

	err := pushImagesIntoRepository(ctx, "localhost:8080/testorg/testrepo", 100000, 1, contentSource)
	//err := pushImagesIntoRepository(ctx, "localhost:5000/testorg/testrepo", 100000, 1, contentSource)
	if err != nil {
		log.Fatal(err)
	}
}
