package main

import (
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

var baseDomainsMap = map[string]string{
	"octet":     "byte",
	"short":     "uint16",
	"long":      "uint32",
	"longlong":  "uint64",
	"timestamp": "time.Time",
	"shortstr":  "string",
	"longstr":   "[]byte",
	"bit":       "bool",
	"table":     "*Table",
}

// Amqp represents root XML-spec structure
type Amqp struct {
	Constants []*Constant `xml:"constant"`
	Domains   []*Domain   `xml:"domain"`
	Classes   []*Class    `xml:"class"`
}

// Constant represents specific constants
type Constant struct {
	Name        string `xml:"name,attr"`
	Value       uint16 `xml:"value,attr"`
	Doc         string `xml:"doc"`
	GoName      string
	GoStr       string
	IgnoreOnMap bool
}

// Domain represent domain amqp types
type Domain struct {
	Name   string `xml:"name,attr"`
	Type   string `xml:"type,attr"`
	GoName string
	GoType string
}

// Class represents collection of amqp-methods
type Class struct {
	Name    string    `xml:"name,attr"`
	ID      uint16    `xml:"index,attr"`
	Methods []*Method `xml:"method"`
	Fields  []*Field  `xml:"field"`
	GoName  string
}

// Method represents amqp method
type Method struct {
	Name        string   `xml:"name,attr"`
	ID          uint16   `xml:"index,attr"`
	Fields      []*Field `xml:"field"`
	GoName      string
	Doc         string `xml:"doc"`
	Synchronous byte   `xml:"synchronous,attr"`
}

// Field represents field inside amqp-method
type Field struct {
	Name        string `xml:"name,attr"`
	Domain      string `xml:"domain,attr"`
	Type        string `xml:"type,attr"`
	GoName      string
	GoType      string
	ReaderFunc  string
	IsBit       bool
	BitOrder    int
	LastBit     bool
	HeaderIndex int
}

// SaveConstants parse and save amqp-constants
func (amqp Amqp) SaveConstants(wr io.Writer) error {
	t := getConstantsTemplate()

	for _, class := range amqp.Classes {
		constant := &Constant{Name: strings.Join([]string{"class", class.Name}, "-"), Value: class.ID, IgnoreOnMap: true}
		amqp.Constants = append(amqp.Constants, constant)

		for _, method := range class.Methods {
			constant := &Constant{Name: strings.Join([]string{"method", class.Name, method.Name}, "-"), Value: method.ID, IgnoreOnMap: true}
			amqp.Constants = append(amqp.Constants, constant)
		}
	}

	for _, constant := range amqp.Constants {
		constant.GoName = kebabToCamel(constant.Name)
		constant.GoStr = kebabToStr(constant.Name)
		constant.Doc = normalizeDoc(constant.GoName+" identifier", constant.Doc)
	}

	return t.Execute(wr, amqp.Constants)
}

// SaveMethods parse and save amqp-methods
func (amqp Amqp) SaveMethods(wr io.Writer) error {
	t := getMethodsTemplate()

	domainAliases := map[string]string{}

	for _, domain := range amqp.Domains {
		if _, ok := baseDomainsMap[domain.Name]; !ok {
			domainAliases[domain.Name] = domain.Type
		}
	}

	for _, class := range amqp.Classes {
		class.GoName = kebabToCamel(class.Name)

		headerIndex := 15
		for _, field := range class.Fields {
			field.GoName = kebabToCamel(field.Name)
			domainKey := calcDomainKey(field, domainAliases)
			field.GoType = baseDomainsMap[domainKey]
			field.ReaderFunc = kebabToCamel(domainKey)
			field.HeaderIndex = headerIndex
			headerIndex--
		}

		for _, method := range class.Methods {
			method.GoName = kebabToCamel(class.Name + "-" + method.Name)
			method.Doc = normalizeDoc(method.GoName, method.Doc)
			bitOrder := 0
			methodsCount := len(method.Fields)
			for idx, field := range method.Fields {
				field.LastBit = false
				field.GoName = kebabToCamel(field.Name)
				domainKey := calcDomainKey(field, domainAliases)
				field.GoType = baseDomainsMap[domainKey]
				field.IsBit = domainKey == "bit"
				if field.IsBit {
					field.BitOrder = bitOrder
					bitOrder++
				} else if bitOrder > 0 {
					method.Fields[idx-1].LastBit = true
					bitOrder = 0
				}

				if field.IsBit && methodsCount == idx+1 {
					method.Fields[idx].LastBit = true
					bitOrder = 0
				}

				field.ReaderFunc = kebabToCamel(domainKey)
			}
		}
	}

	return t.Execute(wr, amqp.Classes)
}

func calcDomainKey(field *Field, domainAliases map[string]string) string {
	var domainKey string

	if field.Domain != "" {
		domainKey = field.Domain
	} else {
		domainKey = field.Type
	}
	if dk, ok := domainAliases[field.Domain]; ok {
		domainKey = dk
	}
	return domainKey
}

func main() {
	file, err := ioutil.ReadFile("protocol/amqp0-9-1.extended.xml")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	var amqp Amqp
	if err := xml.Unmarshal(file, &amqp); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	constantsFile, err := os.Create("amqp/constants_generated.go")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	methodsFile, err := os.Create("amqp/methods_generated.go")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := amqp.SaveConstants(constantsFile); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if err := amqp.SaveMethods(methodsFile); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func kebabToCamel(kebab string) (camel string) {
	parts := strings.Split(kebab, "-")
	for _, part := range parts {
		if part == "id" {
			camel += strings.ToUpper(part)
		} else {
			camel += strings.Title(part)
		}
	}
	return
}

func kebabToStr(kebab string) (str string) {
	parts := strings.Split(kebab, "-")
	var upperParts []string
	for _, part := range parts {
		upperParts = append(upperParts, strings.ToUpper(part))
	}
	return strings.Join(upperParts, "_")
}

func normalizeDoc(goName string, doc string) string {
	doc = strings.TrimSpace(doc)

	var docLines []string

	for idx, line := range strings.Split(doc, "\n") {
		line = strings.TrimSpace(line)
		if idx == 0 {
			line = goName + " " + line
		}
		line = "// " + line
		docLines = append(docLines, line)
	}
	return strings.Join(docLines, "\n")
}
