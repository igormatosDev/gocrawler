package main

import (
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/antchfx/htmlquery"
)

var visited = make(map[string]bool)
var visitedMut = sync.RWMutex{}

var wg sync.WaitGroup
var x int32 = 1
var workers = 150

func main() {
	queue := make(chan string, 100000)

	for i := 1; i <= workers; i++ {
		wg.Add(1)
		go worker(queue)
	}
	queue <- "https://dicasparaviagens.com.br/"
	// queue <- "http://demo.parceirospromo.com.br"
	// queue <- "https://nosnochile.com.br"

	wg.Wait()
}

func worker(queue chan string) {
	for uri := range queue {
		dry, err := enqueue(uri, queue)

		if dry {
			fmt.Println("DRY DRY DRY")
		}

		if err == nil {
			// fmt.Println("DONE fetching: " + uri)
		} else {
			fmt.Printf("----ERROR fetching: %s; error: %v ", uri, err)
		}
	}
	wg.Done()
	close(queue)
}

func enqueue(uri string, queue chan<- string) (dry bool, err error) {
	wg.Add(1)
	// if val, ok := visited[uri]; ok {
	visitedMut.RLock()
	if visited[uri] {
		visitedMut.RUnlock()
		return
	}
	visitedMut.RUnlock()

	// This have a bug
	visitedMut.Lock()
	visited[uri] = true // Record that we're going to visit this page
	visitedMut.Unlock()

	fmt.Println("fetching: " + uri)

	doc, err := htmlquery.LoadURL(uri)
	if err != nil {
		return
	}

	dry = true
	for _, n := range htmlquery.Find(doc, "//a/@href") {
		a := htmlquery.FindOne(n, "//a")
		link := htmlquery.SelectAttr(a, "href")
		link = rescribeLink(uri, link)

		visitedMut.RLock()
		if uri == "" || link == "" || visited[link] {
			visitedMut.RUnlock()
			continue
		}
		visitedMut.RUnlock()

		dry = false

		if !strings.Contains(link, "segurospromo") && !strings.Contains(link, "passagenspromo") {
			queue <- link
		} else {
			fmt.Println("\n" + fmt.Sprintf("%v", x) + " -" + link)
			// THIS MUTEX IS CAUSING THE ERRORS THE EXECUTION IS STOPPING AND IDK THE REASON
			visitedMut.Lock()
			visited[link] = true
			visitedMut.Unlock()

			atomic.AddInt32(&x, 1)

		}
	}
	fmt.Println("DONE: " + uri)
	wg.Done()

	return
}

func fixURL(href, base string) string {
	uri, err := url.Parse(href)
	if err != nil {
		return ""
	}
	baseURL, err := url.Parse(base)
	if err != nil {
		return ""
	}
	uri = baseURL.ResolveReference(uri)
	return uri.String()
}

var ignoredLinks = []string{"wordpress.org", "famethemes.com", "facebook.com"}

func rescribeLink(uri, link string) string {
	if len(link) <= 3 {
		return ""
	} else if strings.HasPrefix(link, "//") {
		return "https:" + link
	} else if strings.HasPrefix(link, "/?") {
		// POSSÍVEL LOOP INFINITO, SEMPRE ADICIONANDO /?P=1(exemplo) NO FINAL DE CADA LINK
		// return uri + link
		return ""

	} else if strings.HasPrefix(link, "#") {
		return ""
	} else if verifyIgnoredLinks(link) {
		return ""
	}
	return link
}

func verifyIgnoredLinks(link string) bool {
	for _, v := range ignoredLinks {
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
