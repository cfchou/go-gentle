// vim:fileencoding=utf-8
package service

import (
	"strings"
	"github.com/hashicorp/golang-lru"
	"golang.org/x/oauth2"
	"net/http"
	"errors"
	"time"
	"golang.org/x/net/context"
)

var ErrNotFound = errors.New("Not found")

type CachedClientsConfig struct {
	CredentialCacheSize int
	ClientCacheSize int
	CredentialEvictionType string
	ClientEvictionType string
}

type CachedClientsService struct {
	CredentialCache Cache
	ClientCache     Cache
}

func (conf *CachedClientsConfig) NewCachedClientsService() (*CachedClientsService, error) {
	var cr_cache, cl_cache Cache
	var err error

	switch strings.ToLower(conf.CredentialEvictionType) {
	case "arc":
		cr_cache, err = lru.NewARC(conf.CredentialCacheSize)
		if err != nil {
			return nil, err
		}
	default:
		cr, err := lru.New(conf.CredentialCacheSize)
		if err != nil {
			return nil, err
		}
		cr_cache = lru_cache{
			Cache: cr,
		}
	}

	switch strings.ToLower(conf.ClientEvictionType) {
	case "arc":
		cl_cache, err = lru.NewARC(conf.ClientCacheSize)
		if err != nil {
			return nil, err
		}
	default:
		cl, err := lru.New(conf.ClientCacheSize)
		if err != nil {
			return nil, err
		}
		cl_cache = lru_cache{
			Cache: cl,
		}
	}
	return &CachedClientsService{
		CredentialCache: cr_cache,
		ClientCache: cl_cache,
	}, nil
}

func (cc *CachedClientsService) GetCredential(id string) (oauth2.Token, error) {
	return nil, ErrNotFound
}

func (cc *CachedClientsService) GetClient(id string) (*http.Client, error) {
	return nil, ErrNotFound
}

func (cc *CachedClientsService) SetCredential(id string, token oauth2.Token) {
}

func (cc *CachedClientsService) SetClient(id string, client *http.Client) {
}

var ErrUnauthorized = errors.New("Unauthorized")
var ErrRequestFailed = errors.New("RequestFailed")


func do_work(client *http.Client) error {
	return nil
}

func refresh_tok(ctx context.Context, refresher *http.Client, id string, timeout time.Duration) (*oauth2.Token, error) {
	req, err := http.NewRequest("GET", "http://exmaple.com", nil)
	if err != nil {
		return nil, err
	}
	creq := req.WithContext(ctx)
	resp, err := refresher.Do(creq)
	if err != nil {
		return nil, err
	}
	return &oauth2.Token{
		AccessToken: "",
		TokenType: "Bearer",
	}, nil
}

func usage(refresher *http.Client, cc *CachedClientsService, id string, accessToken string, timeout time.Duration) {
	context.Background()
	client, err := cc.GetClient(id)
	if err == nil {
		err := do_work(client)
		if err == nil { // succeeded
			tok := oauth2.Token{
				AccessToken:accessToken,
			}
			cc.SetCredential(id, tok)
		} else if err == ErrUnauthorized {
			refresh_tok(refresher, id, )
			// refresh auth
			// create a new client
			// do_work() again

		} else { // failed

		}
	} else { // ErrNotFound
		// create a new client
		// do_work() again
	}
}



