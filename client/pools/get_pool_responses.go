// Code generated by go-swagger; DO NOT EDIT.

package pools

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

// GetPoolReader is a Reader for the GetPool structure.
type GetPoolReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *GetPoolReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewGetPoolOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewGetPoolDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewGetPoolOK creates a GetPoolOK with default headers values
func NewGetPoolOK() *GetPoolOK {
	return &GetPoolOK{}
}

/*
GetPoolOK describes a response with status code 200, with default header values.

Pool
*/
type GetPoolOK struct {
	Payload garm_params.Pool
}

// IsSuccess returns true when this get pool o k response has a 2xx status code
func (o *GetPoolOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this get pool o k response has a 3xx status code
func (o *GetPoolOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this get pool o k response has a 4xx status code
func (o *GetPoolOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this get pool o k response has a 5xx status code
func (o *GetPoolOK) IsServerError() bool {
	return false
}

// IsCode returns true when this get pool o k response a status code equal to that given
func (o *GetPoolOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the get pool o k response
func (o *GetPoolOK) Code() int {
	return 200
}

func (o *GetPoolOK) Error() string {
	return fmt.Sprintf("[GET /pools/{poolID}][%d] getPoolOK  %+v", 200, o.Payload)
}

func (o *GetPoolOK) String() string {
	return fmt.Sprintf("[GET /pools/{poolID}][%d] getPoolOK  %+v", 200, o.Payload)
}

func (o *GetPoolOK) GetPayload() garm_params.Pool {
	return o.Payload
}

func (o *GetPoolOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewGetPoolDefault creates a GetPoolDefault with default headers values
func NewGetPoolDefault(code int) *GetPoolDefault {
	return &GetPoolDefault{
		_statusCode: code,
	}
}

/*
GetPoolDefault describes a response with status code -1, with default header values.

APIErrorResponse
*/
type GetPoolDefault struct {
	_statusCode int

	Payload apiserver_params.APIErrorResponse
}

// IsSuccess returns true when this get pool default response has a 2xx status code
func (o *GetPoolDefault) IsSuccess() bool {
	return o._statusCode/100 == 2
}

// IsRedirect returns true when this get pool default response has a 3xx status code
func (o *GetPoolDefault) IsRedirect() bool {
	return o._statusCode/100 == 3
}

// IsClientError returns true when this get pool default response has a 4xx status code
func (o *GetPoolDefault) IsClientError() bool {
	return o._statusCode/100 == 4
}

// IsServerError returns true when this get pool default response has a 5xx status code
func (o *GetPoolDefault) IsServerError() bool {
	return o._statusCode/100 == 5
}

// IsCode returns true when this get pool default response a status code equal to that given
func (o *GetPoolDefault) IsCode(code int) bool {
	return o._statusCode == code
}

// Code gets the status code for the get pool default response
func (o *GetPoolDefault) Code() int {
	return o._statusCode
}

func (o *GetPoolDefault) Error() string {
	return fmt.Sprintf("[GET /pools/{poolID}][%d] GetPool default  %+v", o._statusCode, o.Payload)
}

func (o *GetPoolDefault) String() string {
	return fmt.Sprintf("[GET /pools/{poolID}][%d] GetPool default  %+v", o._statusCode, o.Payload)
}

func (o *GetPoolDefault) GetPayload() apiserver_params.APIErrorResponse {
	return o.Payload
}

func (o *GetPoolDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}
