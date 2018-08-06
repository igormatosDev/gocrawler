package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antchfx/htmlquery"
	_ "github.com/lib/pq"
)

var visited = make(map[string]bool)
var visitedMut = &sync.RWMutex{}

var wg sync.WaitGroup
var x int32 = 1
var workers = 300

var dbinfo = fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", "postgres", "postgres", "partners")
var db, _ = sql.Open("postgres", dbinfo)

//Result is the main array of links

var allowedLinks = []string{"segurospromo.com", "passagenspromo.com"}

var partnerIDs = make(map[string]int)

var crawlerRun int
var partnerIDTmp int

func main() {
	queue := make(chan string, int(1<<17))

	rowss, _ := db.Query("SELECT max(crawler_run) as crawler_run FROM management_crawlerlink")
	for rowss.Next() {
		rowss.Scan(&crawlerRun)
		crawlerRun++
	}
	fmt.Println(fmt.Sprintf("%v", crawlerRun))

	rows, _ := db.Query("SELECT id as partner_id, website FROM partners_partner")

	for i := 1; i <= workers; i++ {
		wg.Add(1)
		go worker(queue)
	}

	for rows.Next() {
		var partnerID int
		var website string
		rows.Scan(&partnerID, &website)
		fmt.Println("partnerID:" + fmt.Sprintf("%v", partnerID) + " website: " + website)
		enqueueMainLink(queue, website, partnerID)
	}

	// enqueueMainLink(queue, "www.viajarpelomundo.com", 1)
	// enqueueMainLink(queue, "https://dicasparaviagens.com.br/")
	// enqueueMainLink(queue, "https://nosnochile.com.br")

	wg.Wait()

}

func enqueueMainLink(queue chan string, link string, partnerid int) {
	tmpLink := getMainLink(link)
	// if tmpLink != "" {
	queue <- link
	// } else {
	// queue <- "http://" + link
	// }
	allowedLinks = append(allowedLinks, tmpLink)
	partnerIDs[tmpLink] = partnerid
}

func worker(queue chan string) {
loop:
	for {
		select {
		case uri := <-queue:
			err := enqueue(uri, queue)
			if err != nil {
				fmt.Printf("\n\n----ERROR fetching: %s; error: %v ", uri, err)
			}
		case <-time.After(time.Second * 6):
			break loop
		}
	}

	wg.Done()
}

func enqueue(uri string, queue chan<- string) (err error) {
	visitedMut.RLock()
	if visited[uri] {
		visitedMut.RUnlock()
		return
	}
	visitedMut.RUnlock()

	visitedMut.Lock()
	visited[uri] = true
	visitedMut.Unlock()

	// Timeout is set to 20, because some of our partners doesn't have a good server response
	http.DefaultClient.Timeout = time.Second * 20
	doc, err := htmlquery.LoadURL(uri)
	if err != nil {
		fmt.Println("err", err)
		return
	}

	for _, n := range htmlquery.Find(doc, "//a/@href") {
		a := htmlquery.FindOne(n, "//a")
		link := htmlquery.SelectAttr(a, "href")

		visitedMut.RLock()

		mainLink := getMainLink(uri)
		link = verifyLinkRelative(link, mainLink)
		if uri == "" || link == "" || visited[mainLink+link] || !isLinkAllowed(link) {
			visitedMut.RUnlock()
			continue

		}
		visitedMut.RUnlock()

		if strings.Contains(link, "segurospromo") || strings.Contains(link, "passagenspromo") {

			visitedMut.Lock()
			visited[mainLink+link] = true

			partnerIDTmp = partnerIDs[mainLink]

			visitedMut.Unlock()

			params := getLinkParams(link)
			fmt.Println(params)

			_, err = db.Exec("INSERT INTO management_crawlerlink(crawler_run,page,reference_link,utm_source,tags,coupon,partner_id,created_at,partner_slug) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
				crawlerRun, link, uri, params["utm_source"], params["tags"], params["coupon"], partnerIDTmp, "now()", params["pcrslug"])
			if err != nil {
				fmt.Println("------FATAL-------\n", "---", crawlerRun, "---", link, "---", uri, "---", params["utm_source"], "---", params["tags"], "---", params["coupon"], "---", partnerIDTmp, "---", "now()", "---", params["pcrslug"])
				panic(err)
			}

			fmt.Println("\n" + fmt.Sprintf("%v", x) + "(" + mainLink + ")" + " - " + link)
			atomic.AddInt32(&x, 1)
		} else {
			if hasCorrectPrefix(link, mainLink) {
				queue <- link
			}
		}

	}
	return
}

func isLinkAllowed(link string) bool {
	for _, v := range allowedLinks {
		if strings.Contains(link, v) && (strings.HasPrefix(link, "http") || strings.HasPrefix(link, "https") || strings.HasPrefix(link, "//")) {
			return true
		}
	}
	return false
}

func getMainLink(link string) string {
	s := strings.Split(link, "://")
	if len(s) >= 2 {
		s := strings.Split(s[1], "/")
		return s[0]
	}
	return "http://" + s[0]
}

func hasCorrectPrefix(link string, mainLink string) bool {
	if strings.HasPrefix(link, "https://"+mainLink) || strings.HasPrefix(link, "https://www."+mainLink) || strings.HasPrefix(link, "http://"+mainLink) || strings.HasPrefix(link, "http://www."+mainLink) || strings.HasPrefix(link, "https://"+mainLink) || strings.HasPrefix(link, "https://www."+mainLink) || strings.HasPrefix(link, mainLink) {
		return true
	}
	return false
}

func verifyLinkRelative(link string, mainLink string) string {
	if strings.HasPrefix(link, "/") && !strings.HasPrefix(link, "//") {
		return mainLink + link
	}
	return link
}

func getLinkParams(link string) map[string]string {
	var ret = make(map[string]string)
	tmpParams := strings.Split(link, "/?")

	ret["tags"] = "{}"
	ret["pcrslug"] = ""
	ret["utm_source"] = ""
	ret["coupon"] = ""
	ret["pcrid"] = ""
	ret["pcrslug"] = ""

	if len(tmpParams) == 2 {
		params, _ := url.ParseQuery(string(tmpParams[1]))
		if strings.Contains(link, "segurospromo") {
			// NO CASO DO SEGUROS, O PCRSLUG PODE SER CAPTURADO ATRAVÉS DO LINK: /p/{slug}/parceiro
			if len(params["tt"]) > 0 {
				ret["tags"] = "{" + params["tt"][0] + "}"
			}
			if len(params["utm_source"]) > 0 {
				ret["utm_source"] = params["utm_source"][0]
			}
			if len(params["pcrslug"]) > 0 {
				ret["pcrslug"] = params["pcrslug"][0]
			}

			if len(params["cupom"]) > 0 || len(params["coupon"]) > 0 {
				if len(params["cupom"]) > 0 {
					ret["coupon"] = params["cupom"][0]
				}
				if len(params["coupon"]) > 0 {
					ret["coupon"] = params["coupon"][0]
				}
			}

			splitted := strings.Split(link, "/parceiro")
			if len(splitted) == 2 {
				splitted = strings.Split(splitted[0], "/p/")
				if len(splitted) == 2 {
					ret["pcrslug"] = splitted[1]
				}

			}

		} else if strings.Contains(link, "passagenspromo") {

			if len(params["pcrid"]) > 0 {
				ret["pcrid"] = params["pcrid"][0]
			}
			if len(params["utm_source"]) > 0 {
				ret["utm_source"] = params["utm_source"][0]
			}
			if len(params["pcrtt"]) > 0 {
				ret["tags"] = "{" + params["pcrtt"][0] + "}"
			}

		}
		fmt.Println(ret)
	}

	return ret
}

func inArray(val interface{}, array interface{}) (exists bool) {
	exists = false

	switch reflect.TypeOf(array).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(array)

		for i := 0; i < s.Len(); i++ {
			if reflect.DeepEqual(val, s.Index(i).Interface()) == true {
				exists = true
				return
			}
		}
	}

	return
}
