package main

import (
	"strings"
	"fmt"
	"io"
	"compress/gzip"
  "encoding/csv"
  "compress/flate"
	"log"
	"os"
	"strconv"

	"github.com/scritchley/orc"
)

func asString(data string) interface{} {
	return data
}

func asInt(data string) interface{} {
	i, err := strconv.ParseInt(data, 10, 64)
	if err != nil {
		return data
	}
	return i
}

func mappers(schema *orc.TypeDescription) []func(string) interface{} {
  var ret []func(string) interface{}
	for _, ty := range(schema.Types()) {
    switch *ty.Kind {
    case 12:
      continue
    case 3:
      ret = append(ret, asInt)
    default:
      ret = append(ret, asString)
    }  
  }
  return ret
}

func inferType(data string) string {
  _, err := strconv.ParseInt(data, 10, 64)
  if err != nil {
    return "string"
  } 
  return "int"
}

func inferSchema(header []string, record []string) *orc.TypeDescription {
  var types []string
  for idx, val := range(record) {
    types = append(types, header[idx] + ":" + inferType(val))
  }
  schemaDef := fmt.Sprintf("struct<%s>", strings.Join(types, ","))
  schema, err := orc.ParseSchema(schemaDef)

	if err != nil {
		log.Fatal(err)
  }
  return schema
}

func createOrcWriter(schema *orc.TypeDescription) *orc.Writer {
  f, err := os.Create("out.orc")
  writer, err := orc.NewWriter(f, orc.SetSchema(schema), orc.SetCompression(orc.CompressionZlib{Level: flate.BestSpeed}))
  if err != nil {
    log.Fatal(err)
  }
  return writer
}

func main() {
	gf, _ := os.Open("data.csv.gz")
	gr, err := gzip.NewReader(gf)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

  
  var header []string
  var mapperFns []func(string) interface{}
  var schema *orc.TypeDescription
  var writer *orc.Writer
  cr := csv.NewReader(gr)
  
  for {
    rec, err := cr.Read()
    if err == io.EOF {
      break
    }

    if err != nil {
      log.Fatal(err)
    }

    if header == nil {
      header = rec
      continue
    }

    if schema == nil {
      schema = inferSchema(header, rec)
      mapperFns = mappers(schema)
    }

    var converted []interface{}
		for idx, mapper := range(mapperFns) {
      converted = append(converted, mapper(rec[idx]))
    }

    if writer == nil {
      writer = createOrcWriter(schema)
      defer writer.Close()
    }

		writer.Write(converted...)
  }	
}
