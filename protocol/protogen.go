package main

import (
	"io/ioutil"
	"encoding/xml"
	"strings"
	"io"
	"text/template"
	"os"
)

var baseDomainsMap = map[string]string{
	"octet":     "byte",
	"short":     "uint16",
	"long":      "uint32",
	"longlong":  "uint64",
	"timestamp": "uint64",
	"shortstr":  "string",
	"longstr":   "[]byte",
	"bit":       "bool",
	"table":     "*Table",
}

type Amqp struct {
	Constants []*Constant `xml:"constant"`
	Domains   []*Domain   `xml:"domain"`
	Classes   []*Class    `xml:"class"`
}

type Constant struct {
	Name   string `xml:"name,attr"`
	Value  uint16 `xml:"value,attr"`
	GoName string
}

type Domain struct {
	Name   string `xml:"name,attr"`
	Type   string `xml:"type,attr"`
	GoName string
	GoType string
}

type Class struct {
	Name    string    `xml:"name,attr"`
	Id      uint16    `xml:"index,attr"`
	Methods []*Method `xml:"method"`
	GoName  string
}

type Method struct {
	Name   string   `xml:"name,attr"`
	Id     uint16   `xml:"index,attr"`
	Fields []*Field `xml:"field"`
	GoName string
}

type Field struct {
	Name       string `xml:"name,attr"`
	Domain     string `xml:"domain,attr"`
	Type       string `xml:"type,attr"`
	GoName     string
	GoType     string
	ReaderFunc string
	IsBit      bool
	BitOrder   int
	LastBit    bool
}

func (amqp Amqp) SaveConstants(wr io.Writer) {
	const constTemplate = `
package amqp
{{range .}}
const {{.GoName}} = {{.Value}}
{{end}}
`
	t := template.Must(template.New("constTemplate").Parse(constTemplate))

	for _, class := range amqp.Classes {
		constant := &Constant{Name: "class-" + class.Name, Value: class.Id}
		amqp.Constants = append(amqp.Constants, constant)
	}

	for _, constant := range amqp.Constants {
		constant.GoName = kebabToCamel(constant.Name)
	}

	t.Execute(wr, amqp.Constants)
}

func (amqp Amqp) SaveMethods(wr io.Writer) {
	const methodsTemplate = `
package amqp

import ( 
	"io"
	"errors"
	"fmt"
)

type Method interface {
	Name() string
	FrameType() byte
	ClassIdentifier() uint16
	MethodIdentifier() uint16
	Read(reader io.Reader, protoVersion string) (err error)
	Write(writer io.Writer, protoVersion string) (err error)
}
{{range .}}
{{$classId := .Id}}
// {{.GoName}} methods
{{range .Methods}}
type {{.GoName}} struct {
{{range .Fields}}{{.GoName}} {{.GoType}}
{{end}}
}
func (method *{{.GoName}}) Name() string {
    return "{{.GoName}}"
}

func (method *{{.GoName}}) FrameType() byte {
    return 1
}

func (method *{{.GoName}}) ClassIdentifier() uint16 {
    return {{$classId}}
}

func (method *{{.GoName}}) MethodIdentifier() uint16 {
    return {{.Id}}
}

func (method *{{.GoName}}) Read(reader io.Reader, protoVersion string) (err error) {
{{range .Fields}}
	{{if .IsBit }}
	{{if eq .BitOrder 0}}
		bits, err := ReadOctet(reader)
	{{end}}
		method.{{.GoName}} = bits&(1<<{{.BitOrder}}) != 0 
	{{else}}
	method.{{.GoName}}, err = Read{{.ReaderFunc}}(reader{{if eq .ReaderFunc "Table"}}, protoVersion{{end}})
	if err != nil {
		return err
	}
	{{end}}
    
{{end}}
	return
}

func (method *{{.GoName}}) Write(writer io.Writer, protoVersion string) (err error) {
{{$bitFieldsStarted := false}}
{{range .Fields}}
	{{if .IsBit }}
	{{$bitFieldsStarted := true}}
	{{if eq .BitOrder 0}}
	var bits byte
	{{end}}
	if method.{{.GoName}} {
		bits |= 1 << {{.BitOrder}}
	}
	{{if .LastBit}}
	if err = WriteOctet(writer, bits); err != nil {
		return err
	}
	{{end}}
	{{else}}
	if err = Write{{.ReaderFunc}}(writer, method.{{.GoName}}{{if eq .ReaderFunc "Table"}}, protoVersion{{end}}); err != nil {
		return err
	}
	{{end}}
{{end}}
	return
}
{{end}}
{{end}}

func ReadMethod(reader io.Reader, protoVersion string) (Method, error) {
	classId, err := ReadShort(reader)
	if err != nil {
		return nil, err 
	}

	methodId, err := ReadShort(reader)
	if err != nil {
		return nil, err 
	}
	switch classId {
		{{range .}}
	case {{.Id}}:
		switch methodId {
			{{range .Methods}}
		case {{.Id}}:
			var method = &{{.GoName}}{}
			if err := method.Read(reader, protoVersion); err != nil {
				return nil, err
			}
			return method, nil{{end}}
		}{{end}}
	}

	return nil, errors.New(fmt.Sprintf("Unknown classId and methodId: [%d. %d]", classId, methodId))
}

func WriteMethod(writer io.Writer, method Method, protoVersion string) (err error) {
	if err = WriteShort(writer, method.ClassIdentifier()); err != nil {
		return err
	}
	if err = WriteShort(writer, method.MethodIdentifier()); err != nil {
		return err
	}

	if err = method.Write(writer, protoVersion); err != nil {
		return err
	}
	
	return
}
`
	t := template.Must(template.New("methodsTemplate").Parse(methodsTemplate))

	domainAliases := map[string]string{}

	for _, domain := range amqp.Domains {
		if _, ok := baseDomainsMap[domain.Name]; !ok {
			domainAliases[domain.Name] = domain.Type
		}
	}

	for _, class := range amqp.Classes {
		class.GoName = kebabToCamel(class.Name)
		for _, method := range class.Methods {
			method.GoName = kebabToCamel(class.Name + "-" + method.Name)
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

	t.Execute(wr, amqp.Classes)
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
	file, _ := ioutil.ReadFile("protocol/amqp0-9-1.extended.xml")
	var amqp Amqp
	xml.Unmarshal(file, &amqp)

	constantsFile, _ := os.Create("amqp/constants_generated.go")
	methodsFile, _ := os.Create("amqp/methods_generated.go")
	amqp.SaveConstants(constantsFile)
	amqp.SaveMethods(methodsFile)
	return
}

func kebabToCamel(kebab string) (camel string) {
	parts := strings.Split(kebab, "-")
	for _, part := range parts {
		camel += strings.Title(part)
	}
	return
}
