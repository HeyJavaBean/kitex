package http

import (
	"fmt"
	"github.com/cloudwego/gopkg/http/binding"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/utils"
	"net/http"
	"reflect"
)

func BindRequest(recvMsg remote.Message, methodName string, httpReq *http.Request) error {

	if ok := recvMsg.NewData(methodName); !ok {
		return fmt.Errorf("failed to create data for method %s", methodName)
	}

	kitexArgs, ok := recvMsg.Data().(utils.KitexArgs)
	if !ok {
		return fmt.Errorf("failed to assert data for method %s", methodName)
	}

	// only bind the first argument struct ( todo 如果不是 struct 会报错，兼容处理？忽略？)
	firstStruct := kitexArgs.GetFirstArgument()
	if firstStruct == nil {
		return fmt.Errorf("request struct is nil")
	}

	t := reflect.TypeOf(firstStruct)
	// if reqType is nil pointer, then create an empty element for it.
	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		// 不是 struct 的情况下，怎么处理绑定？先忽略
		// todo
		return nil
	}
	// ptr struct 场景
	if reflect.ValueOf(firstStruct).IsNil() {
		newStruct := reflect.New(t.Elem()).Interface()
		msgValue := reflect.ValueOf(recvMsg.Data()).Elem()
		// 第一个参数是 struct 指针，直接 set 即可
		dataField := msgValue.Field(0)
		if dataField.IsValid() && dataField.CanSet() {
			dataField.Set(reflect.ValueOf(newStruct))
			firstStruct = newStruct
		} else {
			return fmt.Errorf("failed to update request struct for method %s", methodName)
		}

	}

	dec, err := binding.NewDecoder(t, &binding.DecodeConfig{})
	if err != nil {
		return fmt.Errorf("failed to create decoder: %w", err)
	}
	requestCtx := binding.NewHTTPRequestContext(httpReq)
	_, err = dec.Decode(requestCtx, firstStruct)
	if err != nil {
		return err
	}

	return nil
}
