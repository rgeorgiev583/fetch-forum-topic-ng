package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

type resourceFetcherContext struct {
	baseURL                  *url.URL
	targetHostDir            string
	dirpath                  string
	fetchedResources         map[string]string // map from the resource URI to the content type of the resource
	replaceResourceReference func(reference string)
}

const failureListFileBasename = "failures.lst"

var cssURLMatcher = regexp.MustCompile(`(url\s*\(["'])(.*?)(["']\))`)

var forumTopicPostStep uint
var forumTopicPageURLBase string
var targetDir string
var isVerboseMode bool

var failureListFilename string
var failureListFile *os.File
var failureListFileMutex sync.Mutex

var workers sync.WaitGroup

func getFailedDownloads(targetDir string) (failedPageNumbers []uint) {
	failedPageNumbers = []uint{}

	failureListFile, err := os.Open(failureListFilename)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: could not open list of failed downloads (%s) for reading", failureListFilename)
		return
	}

	failureListScanner := bufio.NewScanner(failureListFile)
	for failureListScanner.Scan() {
		var failedPageNumber uint
		_, err := fmt.Sscanf(failureListScanner.Text(), "%d", &failedPageNumber)
		if err != nil {
			continue
		}

		failedPageNumbers = append(failedPageNumbers, failedPageNumber)
	}

	failureListFile.Close()

	if len(failedPageNumbers) > 0 {
		fmt.Printf("Found a list of failed downloads (%s); will reattempt them...\n", failureListFilename)
		fmt.Print("Pages for which download will be reattempted: ")
		for i := 0; i < len(failedPageNumbers)-1; i++ {
			fmt.Printf("%d, ", failedPageNumbers[i])
		}
		fmt.Println(failedPageNumbers[len(failedPageNumbers)-1])
	}

	i := 0
	archivedFailureListFilename := fmt.Sprintf("%s.%d", failureListFilename, i)
	for ; err == nil; _, err = os.Stat(archivedFailureListFilename) {
		i++
	}
	if err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "error: could not stat archived list %s of failed downloads\n", archivedFailureListFilename)
		return
	}

	latestFailureListFilename := fmt.Sprintf("%s.%d", failureListFilename, i)
	err = os.Rename(failureListFilename, latestFailureListFilename)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error: could not rename latest list of failed downloads to", latestFailureListFilename)
		return
	}

	return
}

func getResource(urlStr, description string) (contentReader io.ReadCloser, contentType string, err error) {
	response, err := http.Get(urlStr)
	if err != nil {
		log.Printf("error: could not fetch %s: HTTP GET request failed\n", description)
		return
	}
	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("HTTP response received with a non-OK status code")
		log.Printf("error: could not fetch %s: %v\n", description, err)
		return
	}

	contentReader = response.Body
	contentType = response.Header.Get("Content-Type")

	return
}

func adjustResourceFilenameExtension(filename, contentType string) string {
	if strings.HasPrefix(contentType, "text/html") || strings.HasPrefix(contentType, "application/xhtml+xml") {
		filenameEndsWithHTML, _ := filepath.Match("*.[Hh][Tt][Mm][Ll]", filename)
		filenameEndsWithHTM, _ := filepath.Match("*.[Hh][Tt][Mm]", filename)
		if !filenameEndsWithHTML && !filenameEndsWithHTM {
			filename += ".html"
		}
	} else if strings.HasPrefix(contentType, "text/css") {
		filenameEndsWithCSS, _ := filepath.Match("*.[Cc][Ss][Ss]", filename)
		if !filenameEndsWithCSS {
			filename += ".css"
		}
	} else if strings.HasPrefix(contentType, "application/atom+xml") {
		filenameEndsWithAtom, _ := filepath.Match("*.[Aa][Tt][Oo][Mm]", filename)
		if !filenameEndsWithAtom {
			filename += ".atom"
		}
	} else if strings.HasPrefix(contentType, "application/rss+xml") {
		filenameEndsWithRSS, _ := filepath.Match("*.[Rr][Ss][Ss]", filename)
		if !filenameEndsWithRSS {
			filename += ".rss"
		}
	}

	return filename
}

func getLocalResourceRelativeReference(uri *url.URL, contentType string) (relativeReference string) {
	relativeURIReference := url.URL{
		Opaque:   uri.Opaque,
		Path:     uri.Path,
		RawQuery: uri.RawQuery,
	}
	relativeReference = relativeURIReference.String()
	relativeReference = adjustResourceFilenameExtension(relativeReference, contentType)
	return
}

func openFileForResourceContent(resourceURI *url.URL, resourceDescription, contentType, targetHostDir string) (file *os.File, filename string, err error) {
	resourcePath := getLocalResourceRelativeReference(resourceURI, contentType)
	filename = filepath.Join(targetHostDir, filepath.FromSlash(resourcePath))

	dirname := filepath.Dir(filename)
	err = os.MkdirAll(dirname, os.ModePerm)
	if err != nil {
		log.Printf("error: could not create target directory %s for %s\n", dirname, resourceDescription)
		return
	}

	file, err = os.Create(filename)
	if err != nil {
		log.Printf("error: could not create file %s in which to write the content of %s\n", filename, resourceDescription)
		return
	}

	return
}

func fetchResourceFromLinkIfNecessary(linkURI *url.URL, context *resourceFetcherContext) (ok bool) {
	var err error

	resourceDescription := "resource " + linkURI.String()

	if linkURI.Opaque == "" {
		if linkURI.Path == "" {
			return
		}

		linkURI = context.baseURL.ResolveReference(linkURI)
		contentType, wasResourceFetched := context.fetchedResources[linkURI.String()]
		if !wasResourceFetched {
			contentType, err = getAndWriteResourceToFile(linkURI, resourceDescription, context.targetHostDir, context.fetchedResources)
			if err != nil {
				return
			}

			context.fetchedResources[linkURI.String()] = contentType
		}

		relativeLinkPath, err := filepath.Rel(context.dirpath, filepath.FromSlash(linkURI.Path))
		if err != nil {
			log.Println("error: could not determine relative path to resource", linkURI.String())
			return
		}

		relativeReference := filepath.ToSlash(relativeLinkPath)
		if linkURI.RawQuery != "" {
			relativeReference += "%3F" + linkURI.RawQuery
		}
		relativeReference = adjustResourceFilenameExtension(relativeReference, contentType)
		context.replaceResourceReference(relativeReference)
	} else {
		contentType, wasResourceFetched := context.fetchedResources[linkURI.String()]
		if wasResourceFetched {
			contentType, err = getAndWriteResourceToFile(linkURI, resourceDescription, context.targetHostDir, context.fetchedResources)
			if err != nil {
				return
			}

			context.fetchedResources[linkURI.String()] = contentType
		}

		relativeReference := linkURI.Opaque
		if linkURI.RawQuery != "" {
			relativeReference += "%3F" + linkURI.RawQuery
		}
		relativeReference = adjustResourceFilenameExtension(relativeReference, contentType)
		context.replaceResourceReference(relativeReference)
	}

	return true
}

func fetchLinkedResourcesInCSS(css []byte, context *resourceFetcherContext) (rewrittenCSS []byte, err error) {
	var rewrittenCSSBuffer bytes.Buffer

	for urlMatch := cssURLMatcher.FindSubmatchIndex(css); urlMatch != nil; urlMatch = cssURLMatcher.FindSubmatchIndex(css) {
		linkURIStr := string(css[urlMatch[4]:urlMatch[5]])

		linkURI, err := url.Parse(linkURIStr)
		if err != nil {
			log.Println("error: could not parse URL of resource", linkURIStr)
			rewrittenCSSBuffer.Write(css[:urlMatch[1]])
			css = css[urlMatch[1]:]
			continue
		}

		fullContext := *context
		fullContext.replaceResourceReference = func(reference string) {
			rewrittenCSSBuffer.Write(css[urlMatch[2]:urlMatch[3]])
			rewrittenCSSBuffer.Write([]byte(reference))
			rewrittenCSSBuffer.Write(css[urlMatch[6]:urlMatch[7]])
		}

		rewrittenCSSBuffer.Write(css[:urlMatch[0]])
		if !fetchResourceFromLinkIfNecessary(linkURI, &fullContext) {
			rewrittenCSSBuffer.Write(css[urlMatch[0]:urlMatch[1]])
		}
		css = css[urlMatch[1]:]
	}

	rewrittenCSSBuffer.Write(css)
	rewrittenCSS = rewrittenCSSBuffer.Bytes()
	return
}

func getAndWriteResourceToFile(resourceURL *url.URL, resourceDescription, targetHostDir string, fetchedResources map[string]string) (contentType string, err error) {
	contentBody, contentType, err := getResource(resourceURL.String(), resourceDescription)
	if err != nil {
		return
	}
	defer contentBody.Close()

	file, filename, err := openFileForResourceContent(resourceURL, resourceDescription, contentType, targetHostDir)
	defer file.Close()

	if strings.HasPrefix(contentType, "text/css") {
		content, err := ioutil.ReadAll(contentBody)
		if err != nil {
			log.Printf("error: could not read the content of %s successfully\n", resourceDescription)
			return contentType, err
		}

		context := &resourceFetcherContext{
			baseURL:          resourceURL,
			targetHostDir:    targetHostDir,
			dirpath:          filepath.Dir(filepath.FromSlash(resourceURL.Path)),
			fetchedResources: fetchedResources,
		}
		content, err = fetchLinkedResourcesInCSS(content, context)
		if err != nil {
			log.Printf("warning: could not rewrite the links in the content of %s successfully\n", resourceDescription)
		}

		_, err = file.Write(content)
	} else {
		contentBodyReader := bufio.NewReader(contentBody)
		_, err = contentBodyReader.WriteTo(file)
	}
	if err != nil {
		log.Printf("error: could not write the content of %s in file %s successfully\n", resourceDescription, filename)
		return
	}

	return
}

type writer interface {
	io.Writer
	io.ByteWriter
	WriteString(string) (int, error)
}

const escapedChars = "&'<>\"\r"

// shamelessly stolen from "golang.org/x/net/html"
func escape(w writer, s string) error {
	i := strings.IndexAny(s, escapedChars)
	for i != -1 {
		if _, err := w.WriteString(s[:i]); err != nil {
			return err
		}
		var esc string
		switch s[i] {
		case '&':
			esc = "&amp;"
		case '\'':
			// "&#39;" is shorter than "&apos;" and apos was not in HTML until HTML5.
			esc = "&#39;"
		case '<':
			esc = "&lt;"
		case '>':
			esc = "&gt;"
		case '"':
			// "&#34;" is shorter than "&quot;".
			esc = "&#34;"
		case '\r':
			esc = "&#13;"
		default:
			panic("unrecognized escape character")
		}
		s = s[i+1:]
		if _, err := w.WriteString(esc); err != nil {
			return err
		}
		i = strings.IndexAny(s, escapedChars)
	}
	_, err := w.WriteString(s)
	return err
}

func tagStringWithStyleDataPreserved(token *html.Token) string {
	if len(token.Attr) == 0 {
		return token.Data
	}
	buffer := bytes.NewBufferString(token.Data)
	for _, attr := range token.Attr {
		buffer.WriteByte(' ')
		buffer.WriteString(attr.Key)
		buffer.WriteString(`="`)
		if atom.Lookup([]byte(attr.Key)) == atom.Style || strings.HasPrefix(attr.Key, "on") {
			buffer.WriteString(attr.Val)
		} else {
			escape(buffer, attr.Val)
		}
		buffer.WriteByte('"')
	}
	return buffer.String()
}

func tokenStringWithScriptAndStyleDataPreserved(token *html.Token, prevToken *html.Token) string {
	switch token.Type {
	case html.TextToken:
		if prevToken != nil && prevToken.Type == html.StartTagToken && prevToken.DataAtom == atom.Script {
			return token.Data
		}
	case html.StartTagToken:
		return "<" + tagStringWithStyleDataPreserved(token) + ">"
	case html.SelfClosingTagToken:
		return "<" + tagStringWithStyleDataPreserved(token) + "/>"
	}

	return token.String()
}

func fetchForumTopicPage(pageNumber uint, targetDir string) {
	var err error
	defer func() {
		if err != nil {
			failureListFileMutex.Lock()
			failureListFile.WriteString(fmt.Sprintln(pageNumber))
			failureListFileMutex.Unlock()
		}

		workers.Done()
	}()

	postOffset := forumTopicPostStep * (pageNumber - 1)
	pageURLStr := fmt.Sprintf("%s%d", forumTopicPageURLBase, postOffset)

	if isVerboseMode {
		log.Printf("Starting the fetching of page %d into directory %s...\n", pageNumber, targetDir)
		log.Println("URL:", pageURLStr)
	}

	pageURL, err := url.Parse(pageURLStr)
	if err != nil {
		log.Println("error: could not parse URL of page", pageNumber)
		return
	}

	targetHostDir := filepath.Join(targetDir, pageURL.Hostname())

	pageDescription := fmt.Sprint("page", pageNumber)

	contentReader, contentType, err := getResource(pageURL.String(), pageDescription)
	contentTokenizer := html.NewTokenizer(contentReader)
	contentTokenizer.AllowCDATA(true)

	contentFile, contentFilename, err := openFileForResourceContent(pageURL, pageDescription, contentType, targetHostDir)

	pageDirpath := filepath.Dir(filepath.FromSlash(pageURL.Path))

	fetchedResources := map[string]string{}

	var prevToken *html.Token

	for contentTokenizer.Next() != html.ErrorToken {
		func() {
			token := contentTokenizer.Token()

			defer func() {
				_, err := contentFile.WriteString(tokenStringWithScriptAndStyleDataPreserved(&token, prevToken))
				if err != nil {
					log.Printf("error: could not write part of the content of page %d in file %s successfully\n", pageNumber, contentFilename)
				}
				prevToken = &token
			}()

			if token.Type != html.SelfClosingTagToken && token.Type != html.StartTagToken {
				return
			}

			if prevToken.DataAtom == atom.Style {
				context := &resourceFetcherContext{
					baseURL:          pageURL,
					targetHostDir:    targetHostDir,
					dirpath:          pageDirpath,
					fetchedResources: fetchedResources,
				}
				styleData := []byte(token.Data)
				styleData, err = fetchLinkedResourcesInCSS(styleData, context)
				if err != nil {
					log.Printf("error: could not rewrite the links in the content of the `style` element successfully\n")
				}

				token.Data = string(styleData)
			} else {
				var linkURIAttrAtom atom.Atom
				var linkURIAttrIndex, styleIndex int
				var linkURIStr, rel, style string
				var hasLinkURIAttr, hasRel, hasStyle bool
				for index, attr := range token.Attr {
					if hasLinkURIAttr && hasRel {
						break
					}

					attrKeyAtom := atom.Lookup([]byte(attr.Key))
					switch attrKeyAtom {
					case atom.Action, atom.Code, atom.Cite, atom.Data, atom.Formaction, atom.Href, atom.Icon, atom.Manifest, atom.Poster, atom.Src, atom.Srcset, atom.Usemap:
						linkURIAttrAtom, linkURIAttrIndex, linkURIStr, hasLinkURIAttr = attrKeyAtom, index, attr.Val, true

					case atom.Rel:
						rel, hasRel = attr.Val, true

					case atom.Style:
						styleIndex, style, hasStyle = index, attr.Val, true

					default:
						switch attr.Key {
						case "archive", "background", "codebase", "classid", "lowsrc", "longdesc", "profile":
							linkURIAttrIndex, linkURIStr, hasLinkURIAttr = index, attr.Val, true
						}
					}
				}

				if hasStyle {
					context := &resourceFetcherContext{
						baseURL:          pageURL,
						targetHostDir:    targetHostDir,
						dirpath:          pageDirpath,
						fetchedResources: fetchedResources,
					}
					styleData := []byte(style)
					styleData, err = fetchLinkedResourcesInCSS(styleData, context)
					if err != nil {
						log.Printf("error: could not rewrite the links in the content of the `style` attribute successfully\n")
					}

					token.Attr[styleIndex].Val = string(styleData)
				}

				if !hasLinkURIAttr {
					return
				}

				linkURI, err := url.Parse(linkURIStr)
				if err != nil {
					log.Println("error: could not parse URL of resource", linkURIStr)
					return
				}

				isRelInline := strings.Contains(rel, "stylesheet") || strings.Contains(rel, "icon") || strings.Contains(rel, "shortcut")
				if linkURIAttrAtom != atom.Action && linkURIAttrAtom != atom.Formaction && (linkURIAttrAtom != atom.Href || token.DataAtom != atom.A && token.DataAtom != atom.Area && token.DataAtom != atom.Embed && (token.DataAtom != atom.Link || hasRel && isRelInline)) {
					context := &resourceFetcherContext{
						baseURL:          pageURL,
						targetHostDir:    targetHostDir,
						dirpath:          pageDirpath,
						fetchedResources: fetchedResources,
						replaceResourceReference: func(reference string) {
							token.Attr[linkURIAttrIndex].Val = reference
						},
					}
					fetchResourceFromLinkIfNecessary(linkURI, context)
				} else {
					linkURI = pageURL.ResolveReference(linkURI)

					token.Attr[linkURIAttrIndex].Val = linkURI.String()
				}
			}
		}()
	}

	contentFile.Close()
	contentReader.Close()

	if isVerboseMode {
		log.Printf("Finished the fetching of page %d.\n", pageNumber)
	}
}

func main() {
	const forumTopicMinPageNumber uint = 1

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `usage: %s [-f] [-s posts] [-t directory] [-v] URL [page ranges]

Before doing anything else, this script tries to fetch again pages which could not be downloaded successfully during its last run.
The purpose of this script is to download all pages in the specified ranges from the desired forum topic according to the provided base template URL.
A page range specification looks like this: `+"`"+`first..last`+"`"+`, where `+"`"+`first`+"`"+` is the number of the first page and
`+"`"+`last`+"`"+` is the number of the last one.
If no page ranges are specified, no new pages will be fetched; nevertheless, failed downloads will still be re-attempted.

Flags:
`, os.Args[0])
		flag.PrintDefaults()
	}

	force := false
	flag.BoolVar(&force, "f", force, "enable overwriting of already fetched pages")

	//spanHosts := false
	//flag.BoolVar(&spanHosts, "H", spanHosts, "enable spanning across hosts when doing recursive fetching of a page")

	forumTopicPostStep = 15
	flag.UintVar(&forumTopicPostStep, "s", forumTopicPostStep, "number of `posts` contained on a single page; used for determining the offset of the current page in the URL parameters")

	targetDir, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error: could not get current working directory")
		os.Exit(3)
	}
	flag.StringVar(&targetDir, "t", targetDir, "`directory` where the pages will be downloaded")

	isVerboseMode = false
	flag.BoolVar(&isVerboseMode, "v", isVerboseMode, "enable outputting of verbose messages")

	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "error: no base URL specified for forum topic pages")
		fmt.Fprintf(os.Stderr, "Run '%s -h' for usage.\n", os.Args[0])
		os.Exit(1)
	}

	forumTopicPageURLBase = args[0]

	failureListFilename := filepath.Join(targetDir, failureListFileBasename)

	failedPageNumbers := map[uint]struct{}{}
	for _, failedPageNumber := range getFailedDownloads(targetDir) {
		failedPageNumbers[failedPageNumber] = struct{}{}
	}

	forumTopicPageNumbers := map[uint]struct{}{}
	for failedPageNumber := range failedPageNumbers {
		forumTopicPageNumbers[failedPageNumber] = struct{}{}
	}

	for i := 1; i < len(args); i++ {
		forumTopicPageRange := args[i]
		var forumTopicPageRangeStart, forumTopicPageRangeEnd uint
		_, err := fmt.Sscanf(forumTopicPageRange, "%d..%d", &forumTopicPageRangeStart, &forumTopicPageRangeEnd)
		if err != nil {
			forumTopicPageRangeStart = forumTopicMinPageNumber
			_, err = fmt.Sscanf(forumTopicPageRange, "%d", &forumTopicPageRangeEnd)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "error: invalid page range specification:", forumTopicPageRange)
			fmt.Fprintf(os.Stderr, "Run '%s -h' for usage.\n", os.Args[0])
			os.Exit(1)
		}

		for j := forumTopicPageRangeStart; j <= forumTopicPageRangeEnd; j++ {
			forumTopicPageNumbers[j] = struct{}{}
		}
	}

	if len(forumTopicPageNumbers) == 0 {
		fmt.Fprintln(os.Stderr, "error: no range of forum topic pages specified")
		fmt.Fprintf(os.Stderr, "Run '%s -h' for usage.\n", os.Args[0])
		os.Exit(1)
	}

	failureListFile, err = os.Create(failureListFilename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: could not create file %s in which to log failed downloads\n", failureListFilename)
		return
	}
	defer failureListFile.Close()

	for forumTopicPageNumber := range forumTopicPageNumbers {
		forumTopicPageTargetDir := filepath.Join(targetDir, fmt.Sprint(forumTopicPageNumber))

		if !force {
			forumTopicPageTargetDirStat, err := os.Stat(forumTopicPageTargetDir)
			if err != nil && !os.IsNotExist(err) {
				log.Printf("error: could not stat target directory %s for page %d\n", forumTopicPageTargetDir, forumTopicPageNumber)
				continue
			} else if err == nil && forumTopicPageTargetDirStat.IsDir() {
				_, ok := failedPageNumbers[forumTopicPageNumber]
				if !ok {
					continue
				}
			}
		}
		workers.Add(1)
		go fetchForumTopicPage(forumTopicPageNumber, forumTopicPageTargetDir)
	}

	workers.Wait()
}
