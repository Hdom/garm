// Code generated by go-swagger; DO NOT EDIT.

package providers

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	apiserver_params "github.com/cloudbase/garm/apiserver/params"
	garm_params "github.com/cloudbase/garm/params"
)

// ListProvidersReader is a Reader for the ListProviders structure.
type ListProvidersReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ListProvidersReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewListProvidersOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 400:
		result := NewListProvidersBadRequest()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("[GET /providers] ListProviders", response, response.Code())
	}
}

// NewListProvidersOK creates a ListProvidersOK with default headers values
func NewListProvidersOK() *ListProvidersOK {
	return &ListProvidersOK{}
}

/*
ListProvidersOK describes a response with status code 200, with default header values.

Providers
*/
type ListProvidersOK struct {
	Payload garm_params.Providers
}

// IsSuccess returns true when this list providers o k response has a 2xx status code
func (o *ListProvidersOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this list providers o k response has a 3xx status code
func (o *ListProvidersOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this list providers o k response has a 4xx status code
func (o *ListProvidersOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this list providers o k response has a 5xx status code
func (o *ListProvidersOK) IsServerError() bool {
	return false
}

// IsCode returns true when this list providers o k response a status code equal to that given
func (o *ListProvidersOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the list providers o k response
func (o *ListProvidersOK) Code() int {
	return 200
}

func (o *ListProvidersOK) Error() string {
	return fmt.Sprintf("[GET /providers][%d] listProvidersOK  %+v", 200, o.Payload)
}

func (o *ListProvidersOK) String() string {
	return fmt.Sprintf("[GET /providers][%d] listProvidersOK  %+v", 200, o.Payload)
}

func (o *ListProvidersOK) GetPayload() garm_params.Providers {
	return o.Payload
}

func (o *ListProvidersOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewListProvidersBadRequest creates a ListProvidersBadRequest with default headers values
func NewListProvidersBadRequest() *ListProvidersBadRequest {
	return &ListProvidersBadRequest{}
}

/*
ListProvidersBadRequest describes a response with status code 400, with default header values.

APIErrorResponse
*/
type ListProvidersBadRequest struct {
	Payload apiserver_params.APIErrorResponse
}

// IsSuccess returns true when this list providers bad request response has a 2xx status code
func (o *ListProvidersBadRequest) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this list providers bad request response has a 3xx status code
func (o *ListProvidersBadRequest) IsRedirect() bool {
	return false
}

// IsClientError returns true when this list providers bad request response has a 4xx status code
func (o *ListProvidersBadRequest) IsClientError() bool {
	return true
}

// IsServerError returns true when this list providers bad request response has a 5xx status code
func (o *ListProvidersBadRequest) IsServerError() bool {
	return false
}

// IsCode returns true when this list providers bad request response a status code equal to that given
func (o *ListProvidersBadRequest) IsCode(code int) bool {
	return code == 400
}

// Code gets the status code for the list providers bad request response
func (o *ListProvidersBadRequest) Code() int {
	return 400
}

func (o *ListProvidersBadRequest) Error() string {
	return fmt.Sprintf("[GET /providers][%d] listProvidersBadRequest  %+v", 400, o.Payload)
}

func (o *ListProvidersBadRequest) String() string {
	return fmt.Sprintf("[GET /providers][%d] listProvidersBadRequest  %+v", 400, o.Payload)
}

func (o *ListProvidersBadRequest) GetPayload() apiserver_params.APIErrorResponse {
	return o.Payload
}

func (o *ListProvidersBadRequest) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}
