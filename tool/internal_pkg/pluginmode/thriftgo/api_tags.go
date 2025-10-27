package thriftgo

import (
	"fmt"
	"github.com/cloudwego/thriftgo/parser"
	"strings"
)

var apiTags = map[string]string{
	"api.query":  "query",
	"api.header": "header",
	"api.form":   "form",
	"api.cookie": "cookie",
	"api.path":   "path",
	"api.body":   "body", // api.body means body and json
}

func getTagString(f *parser.Field) (string, bool) {

	var tags map[string]string
	for _, anno := range f.GetAnnotations() {
		tag, ok := apiTags[anno.GetKey()]
		if ok {
			if tags == nil {
				tags = map[string]string{}
			}
			values := anno.GetValues()[0]
			// split by ',' and get first one as tag value
			tags[tag] = strings.Split(values, ",")[0]
		}
	}

	// no need to append insert point if no tag found
	if len(tags) == 0 {
		return "", false
	}

	// if has form tag and body tag, then use form tag to overwrite api.body tag
	if _, ok := tags["body"]; ok {
		if _, ok = tags["form"]; !ok {
			tags["form"] = tags["body"]
		}
	}

	var sb strings.Builder

	// sort tags by key: path > form > query > cookie > header
	priorityOrder := []string{"path", "form", "query", "cookie", "header"}

	for _, key := range priorityOrder {
		if v, exists := tags[key]; exists {
			sb.WriteString(fmt.Sprintf(` %s:"%s"`, key, v))
		}
	}

	return sb.String(), true
}
