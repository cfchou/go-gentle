// vim:fileencoding=utf-8
package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path/filepath"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"
	"time"
	"github.com/hashicorp/go-cleanhttp"
	"github.com/hashicorp/golang-lru"
)

// getClient uses a Context and Config to retrieve a Token
// then generate a Client. It returns the generated Client.
func getClient(ctx context.Context, config *oauth2.Config) *http.Client {
	cacheFile, err := tokenCacheFile()
	if err != nil {
		log.Fatalf("Unable to get path to cached credential file. %v", err)
	}
	tok, err := tokenFromFile(cacheFile)
	if err != nil {
		log.Printf("tokeFromFile error. %v", err)
		tok = getTokenFromWeb(config)
		saveToken(cacheFile, tok)
	}
	return config.Client(ctx, tok)
}

// getTokenFromWeb uses Config to request a Token.
// It returns the retrieved Token.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var code string
	if _, err := fmt.Scan(&code); err != nil {
		log.Fatalf("Unable to read authorization code %v", err)
	}

	tok, err := config.Exchange(oauth2.NoContext, code)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web %v", err)
	}
	return tok
}

// tokenCacheFile generates credential file path/filename.
// It returns the generated credential path/filename.
func tokenCacheFile0() (string, error) {
	usr, err := user.Current()
	if err != nil {
		return "", err
	}
	tokenCacheDir := filepath.Join(usr.HomeDir, ".credentials")

	os.MkdirAll(tokenCacheDir, 0700)
	return filepath.Join(tokenCacheDir,
		url.QueryEscape("gmail-go-quickstart.json")), err
}

func tokenCacheFile1(name string) (string, error) {
	//tokenCacheDir := filepath.Join(usr.HomeDir, ".credentials")
	tokenCacheDir, err := filepath.Abs(".")
	if err != nil {
		return "", nil
	}

	os.MkdirAll(tokenCacheDir, 0700)
	return filepath.Join(tokenCacheDir,
		url.QueryEscape(name)), err
}

// tokenCacheFile generates credential file path/filename.
// It returns the generated credential path/filename.
func tokenCacheFile() (string, error) {
	//tokenCacheDir := filepath.Join(usr.HomeDir, ".credentials")
	tokenCacheDir, err := filepath.Abs(".")
	if err != nil {
		return "", nil
	}

	os.MkdirAll(tokenCacheDir, 0700)
	return filepath.Join(tokenCacheDir,
		url.QueryEscape("gmail-go-quickstart.json")), err
}
// tokenFromFile retrieves a Token from a given file path.
// It returns the retrieved Token and any read error encountered.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	t := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(t)
	defer f.Close()
	return t, err
}

// saveToken uses a file path to create a file and store the
// token in it.
func saveToken(file string, token *oauth2.Token) {
	fmt.Printf("Saving credential file to: %s\n", file)
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatalf("Unable to cache oauth token: %v", err)
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
}






func showLabels(srv *gmail.Service) {
	user := "me"
	r, err := srv.Users.Labels.List(user).Do()
	if err != nil {
		log.Fatalf("Unable to retrieve labels. %v", err)
	}
	if (len(r.Labels) > 0) {
		fmt.Print("Labels:\n")
		for _, l := range r.Labels {
			fmt.Printf("- %s\n",  l.Name)
		}
	} else {
		fmt.Print("No labels found.")
	}
}

func getToken(tok_file string) *oauth2.Token {
	cacheFile, err := tokenCacheFile1(tok_file)
	if err != nil {
		log.Fatalf("Unable to get path to cached credential file. %v", err)

	}
	tok, err := tokenFromFile(cacheFile)
	if err != nil {
		log.Fatalf("tokenFromFile. %v", err)
	}
	return tok
}

func createService(name string, conf *oauth2.Config, tok *oauth2.Token, cl *http.Client, cache *lru.Cache) {
	parent_ctx := context.Background()
	ctx := context.WithValue(parent_ctx, oauth2.HTTPClient, cl)
	client := conf.Client(ctx, tok)
	// Timeout for a request
	client.Timeout = time.Second * 30
	srv, err := gmail.New(client)
	if err != nil {
		log.Fatalf("Unable to retrieve gmail Client %v", err)
	}
	cache.Add(name, srv)
}

func getService(name string, cache *lru.Cache) *gmail.Service {
	v, b := cache.Get(name)
	if !b {
		log.Fatalf("lru.Get() false")
	}
	return v.(*gmail.Service)
}

func main() {
	cache, err := lru.New(2)
	if err != nil {
		log.Fatalf("lru.New(). %v", err)
	}

	// It makes more sense to have separated sharable Transports, i.e.
	// One Transport shared by all Clients of Gmail.
	// Another Transport shared by Clients of another service.
	tr := cleanhttp.DefaultPooledTransport()
	// Enlarge MaxIdleConnsPerHost so that connections to the same Gmail
	// host are cached. It should be less than MaxIdleConns.
	tr.MaxIdleConnsPerHost = 100
	tr.MaxIdleConns = 200
	tr.IdleConnTimeout = 20 * time.Minute

	// This Client will not really be used in oauth2. oauth2 only extract Transport
	// from it if it's saved as context's oauth2.HTTPClient.
	cl := &http.Client{
		Transport:tr,
	}


	bs, err := ioutil.ReadFile("client_secret.json")
	config, err := google.ConfigFromJSON(bs, gmail.GmailReadonlyScope)

	tok1 := getToken("gmail-go-quickstart1.json")
	tok2 := getToken("gmail-go-quickstart2.json")


	createService("srv1", config, tok1, cl, cache)
	srv1 := getService("srv1", cache)
	showLabels(srv1)
	fmt.Println("===========================")

	createService("srv2", config, tok2, cl, cache)
	srv2 := getService("srv2", cache)
	showLabels(srv2)
	fmt.Println("===========================")

	srv1a := getService("srv1", cache)
	showLabels(srv1a)
}

func main2() {

	// create user token
	cacheFile, err := tokenCacheFile()
	if err != nil {
		log.Fatalf("Unable to get path to cached credential file. %v", err)

	}
	tok, err := tokenFromFile(cacheFile)
	if err != nil {
		log.Fatalf("tokenFromFile. %v", err)
	}

	// create client secret
	b, err := ioutil.ReadFile("client_secret.json")
	config, err := google.ConfigFromJSON(b, gmail.GmailReadonlyScope)

	// new client with user token and client secret
	ctx := context.Background()
	client := config.Client(ctx, tok)
	client.Timeout = time.Second * 30

	// new gmail service with client
	srv, err := gmail.New(client)
	if err != nil {
		log.Fatalf("Unable to retrieve gmail Client %v", err)
	}

	user := "me"
	r, err := srv.Users.Labels.List(user).Do()
	if err != nil {
		log.Fatalf("Unable to retrieve labels. %v", err)
	}
	if (len(r.Labels) > 0) {
		fmt.Print("Labels:\n")
		for _, l := range r.Labels {
			fmt.Printf("- %s\n",  l.Name)
		}
	} else {
		fmt.Print("No labels found.")
	}
}

func main3() {
	ctx := context.Background()

	b, err := ioutil.ReadFile("client_secret.json")
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	// If modifying these scopes, delete your previously saved credentials
	// at ~/.credentials/gmail-go-quickstart.json
	config, err := google.ConfigFromJSON(b, gmail.GmailReadonlyScope)
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	client := getClient(ctx, config)
	client.Timeout = time.Second * 30

	srv, err := gmail.New(client)
	if err != nil {
		log.Fatalf("Unable to retrieve gmail Client %v", err)
	}

	user := "me"
	r, err := srv.Users.Labels.List(user).Do()
	if err != nil {
		log.Fatalf("Unable to retrieve labels. %v", err)
	}
	if (len(r.Labels) > 0) {
		fmt.Print("Labels:\n")
		for _, l := range r.Labels {
			fmt.Printf("- %s\n",  l.Name)
		}
	} else {
		fmt.Print("No labels found.")
	}

}

