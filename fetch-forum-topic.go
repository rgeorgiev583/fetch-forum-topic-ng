package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

const failureListFileBasename = "failures.lst"

var forumTopicPostStep uint
var forumTopicPageURLTemplate string
var targetDir string
var isVerboseMode bool

var failureListFilename string
var failureListFile *os.File
var failureListFileMutex sync.Mutex

//var workers sync.WaitGroup

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

func getResource(urlStr string, description string) (contentReader io.ReadCloser, contentType string, err error) {
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

func adjustResourceFilenameExtension(filename string, contentType string) string {
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

func getLocalResourceRelativeReference(resourceURL *url.URL, contentType string) (relativeReference string) {
	relativeURIReference := url.URL{
		Opaque:   resourceURL.Opaque,
		Path:     resourceURL.Path,
		RawQuery: resourceURL.RawQuery,
	}
	relativeReference = relativeURIReference.String()
	relativeReference = adjustResourceFilenameExtension(relativeReference, contentType)
	return
}

func writeResourceContentToFile(content []byte, contentType string, resourceURL *url.URL, resourceDescription, targetHostDir string) (err error) {
	resourcePath := getLocalResourceRelativeReference(resourceURL, contentType)
	filename := filepath.Join(targetHostDir, filepath.FromSlash(resourcePath))

	dirname := filepath.Dir(filename)
	err = os.MkdirAll(dirname, os.ModePerm)
	if err != nil {
		log.Printf("error: could not create target directory %s for %s\n", dirname, resourceDescription)
		return
	}

	file, err := os.Create(filename)
	if err != nil {
		log.Printf("error: could not create file %s in which to write the content of %s\n", filename, resourceDescription)
		return
	}
	defer file.Close()

	_, err = file.Write(content)
	if err != nil {
		log.Printf("error: could not write the content of %s in file %s successfully\n", resourceDescription, filename)
		return
	}

	return
}

func getAndWriteResourceToFile(resourceURL *url.URL, resourceDescription, targetHostDir string) (contentType string, err error) {
	contentBody, contentType, err := getResource(resourceURL.String(), resourceDescription)
	if err != nil {
		return
	}
	defer contentBody.Close()

	content, err := ioutil.ReadAll(contentBody)
	if err != nil {
		log.Printf("error: could not fetch %s: could not read content of HTTP response body\n", resourceDescription)
		return
	}

	err = writeResourceContentToFile(content, contentType, resourceURL, resourceDescription, targetHostDir)
	if err != nil {
		return
	}

	return
}

func fetchForumTopicPage(pageNumber uint, targetDir string) {
	var err error
	defer func() {
		if err != nil {
			failureListFileMutex.Lock()
			failureListFile.WriteString(fmt.Sprintln(pageNumber))
			failureListFileMutex.Unlock()
		}

		//workers.Done()
	}()

	postOffset := forumTopicPostStep * (pageNumber - 1)
	pageURLStr := fmt.Sprintf("%s%d", forumTopicPageURLTemplate, postOffset)

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
	document, err := html.Parse(contentReader)
	if err != nil {
		log.Println("error: could not parse HTML content of page", pageNumber)
		return
	}
	contentReader.Close()

	pageDirpath := filepath.Dir(filepath.FromSlash(pageURL.Path))

	var fetchRequisiteResourcesAndRewriteLinks func(node *html.Node)
	recursivelyFetchRequisiteResourcesAndRewriteLinks := func(node *html.Node) {
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			fetchRequisiteResourcesAndRewriteLinks(child)
		}
	}
	fetchRequisiteResourcesAndRewriteLinks = func(node *html.Node) {
		defer recursivelyFetchRequisiteResourcesAndRewriteLinks(node)

		if node.Type != html.ElementNode {
			return
		}

		var linkURIAttrAtom atom.Atom
		var linkURIAttrIndex int
		var linkURIStr, rel string
		var hasLinkURIAttr, hasRel bool
		for index, attr := range node.Attr {
			if hasLinkURIAttr && hasRel {
				break
			}

			attrKeyAtom := atom.Lookup([]byte(attr.Key))
			switch attrKeyAtom {
			case atom.Action, atom.Code, atom.Cite, atom.Data, atom.Formaction, atom.Href, atom.Icon, atom.Manifest, atom.Poster, atom.Src, atom.Srcset, atom.Usemap:
				linkURIAttrAtom, linkURIAttrIndex, linkURIStr, hasLinkURIAttr = attrKeyAtom, index, attr.Val, true

			case atom.Rel:
				rel, hasRel = attr.Val, true

			default:
				switch attr.Key {
				case "archive", "background", "codebase", "classid", "lowsrc", "longdesc", "profile":
					linkURIAttrIndex, linkURIStr, hasLinkURIAttr = index, attr.Val, true
				}
			}
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
		if linkURIAttrAtom != atom.Action && linkURIAttrAtom != atom.Formaction && (linkURIAttrAtom != atom.Href || node.DataAtom != atom.A && node.DataAtom != atom.Area && node.DataAtom != atom.Embed && (node.DataAtom != atom.Link || hasRel && isRelInline)) {
			resourceDescription := "resource " + linkURIStr

			if linkURI.Opaque == "" {
				if linkURI.Path != "" {
					linkURI = pageURL.ResolveReference(linkURI)

					contentType, err := getAndWriteResourceToFile(linkURI, resourceDescription, targetHostDir)
					if err != nil {
						return
					}

					relativeLinkPath, err := filepath.Rel(pageDirpath, filepath.FromSlash(linkURI.Path))
					if err != nil {
						log.Println("error: could not determine relative path to resource", linkURIStr)
						return
					}

					relativeReference := filepath.ToSlash(relativeLinkPath)
					if linkURI.RawQuery != "" {
						relativeReference += "%3F" + linkURI.RawQuery
					}
					relativeReference = adjustResourceFilenameExtension(relativeReference, contentType)
					node.Attr[linkURIAttrIndex].Val = relativeReference
				}
			} else {
				contentType, err := getAndWriteResourceToFile(linkURI, resourceDescription, targetHostDir)
				if err != nil {
					return
				}

				relativeReference := linkURI.Opaque
				if linkURI.RawQuery != "" {
					relativeReference += "%3F" + linkURI.RawQuery
				}
				relativeReference = adjustResourceFilenameExtension(relativeReference, contentType)
				node.Attr[linkURIAttrIndex].Val = relativeReference
			}
		} else {
			linkURI = pageURL.ResolveReference(linkURI)

			node.Attr[linkURIAttrIndex].Val = linkURI.String()
		}
	}
	recursivelyFetchRequisiteResourcesAndRewriteLinks(document)

	var pageContentWriter strings.Builder
	err = html.Render(&pageContentWriter, document)
	if err != nil {
		log.Printf("error: could not render HTML content of page %d after rewriting of links\n", pageNumber)
		return
	}

	pageContentAfterRewrite := pageContentWriter.String()
	err = writeResourceContentToFile([]byte(pageContentAfterRewrite), contentType, pageURL, pageDescription, targetHostDir)
	if err != nil {
		return
	}

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

	forumTopicPageURLTemplate = args[0]

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
		//workers.Add(1)
		fetchForumTopicPage(forumTopicPageNumber, forumTopicPageTargetDir)
	}

	//workers.Wait()
}