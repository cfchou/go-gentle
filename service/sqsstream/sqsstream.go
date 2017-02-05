// vim:fileencoding=utf-8
package service

import "github.com/inconshreveable/log15"

var Log = log15.New()

func init()  {
	Log.SetHandler(log15.DiscardHandler())
}
