package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/antchfx/htmlquery"
)

var visited = make(map[string]bool)
var visitedMut = sync.RWMutex{}

var wg sync.WaitGroup
var x int32 = 1
var workers = 20

//Result is the main array of links

var mydata = map[string][]string{}

var ignoredLinks = []string{"wordpress.org", "famethemes.com", "facebook.com"}
var allowedLinks = []string{"segurospromo.com", "passagenspromo.com"}

func main() {
	queue := make(chan string, int(1<<17))

	for i := 1; i <= workers; i++ {
		wg.Add(1)
		go worker(queue)
	}

	enqueueMainLink(queue, "http://localhost/wordpress-teste")
	// enqueueMainLink(queue, "http://demo.parceirospromo.com.br")
	// enqueueMainLink(queue, "https://dicasparaviagens.com.br/")
	// enqueueMainLink(queue, "https://nosnochile.com.br")
	wg.Wait()

}

func enqueueMainLink(queue chan string, link string) {
	tmpLink := getMainLink(link)
	if tmpLink != "" {
		queue <- link
		allowedLinks = append(allowedLinks, tmpLink)
	} else {
		fmt.Println("LINK ", link, " NÃO FOI ENFILEIRADO")
	}
}

func worker(queue chan string) {
	for uri := range queue {
		dry, err := enqueue(uri, queue)

		if dry {
			// fmt.Println("DRY DRY DRY")
		}
		if err == nil {
			// fmt.Println("DONE fetching: " + uri)
		} else {
			// fmt.Printf("----ERROR fetching: %s; error: %v ", uri, err)
		}
	}

	wg.Done()
	// close(queue)
}

func enqueue(uri string, queue chan<- string) (dry bool, err error) {
	wg.Add(1)

	visitedMut.RLock()
	if visited[uri] {
		visitedMut.RUnlock()
		return
	}
	visitedMut.RUnlock()

	visitedMut.Lock()
	visited[uri] = true
	visitedMut.Unlock()

	// fmt.Println("fetching: " + uri)

	doc, err := htmlquery.LoadURL(uri)
	if err != nil {
		return
	}

	dry = true
	for _, n := range htmlquery.Find(doc, "//a/@href") {
		a := htmlquery.FindOne(n, "//a")
		link := htmlquery.SelectAttr(a, "href")

		visitedMut.RLock()
		mainLink := getMainLink(uri)
		if uri == "" || link == "" || visited[mainLink+link] || !isLinkAllowed(link) {
			visitedMut.RUnlock()
			continue

		}
		visitedMut.RUnlock()

		dry = false

		if isLinkAllowed(link) {
			if strings.Contains(link, "segurospromo") || strings.Contains(link, "passagenspromo") {

				visitedMut.Lock()
				visited[mainLink+link] = true
				mydata[mainLink] = append(mydata[mainLink], link)
				visitedMut.Unlock()

				fmt.Println("\n" + fmt.Sprintf("%v", x) + "(" + mainLink + ")" + " - " + link)
				jsonString, _ := json.Marshal(mydata)
				fmt.Println("\n\n\n\n\n" + string(jsonString))
				atomic.AddInt32(&x, 1)
			} else {
				queue <- link
			}
		}

	}
	// fmt.Println("DONE: " + uri)

	wg.Done()

	return
}

func isLinkAllowed(link string) bool {
	for _, v := range allowedLinks {
		if strings.Contains(link, v) {
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
	return ""
}
