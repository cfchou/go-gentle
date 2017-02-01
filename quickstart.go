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

func main() {
	get_token := func(name string) *oauth2.Token {
		cacheFile, err := tokenCacheFile1(name)
		if err != nil {
			log.Fatalf("Unable to get path to cached credential file. %v", err)

		}
		tok, err := tokenFromFile(cacheFile)
		if err != nil {
			log.Fatalf("tokenFromFile. %v", err)
		}
		return tok
	}
	get_client := func(conf *oauth2.Config, tok *oauth2.Token) *http.Client {
		ctx := context.Background()
		client := conf.Client(ctx, tok)
		client.Timeout = time.Second * 30
		return client
	}

	show_labels := func(srv *gmail.Service) {
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

	tok1 := get_token("gmail_1.json")
	tok2 := get_token("gmail_2.json")

	b, err := ioutil.ReadFile("client_secret.json")
	config, err := google.ConfigFromJSON(b, gmail.GmailReadonlyScope)

	c1 := get_client(config, tok1)
	c2 := get_client(config, tok2)

	srv1, err := gmail.New(c1)
	if err != nil {
		log.Fatalf("Unable to retrieve gmail Client %v", err)
	}

	srv2, err := gmail.New(c2)
	if err != nil {
		log.Fatalf("Unable to retrieve gmail Client %v", err)
	}

	fmt.Println("===========================")
	show_labels(srv1)
	fmt.Println("===========================")
	show_labels(srv2)

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

