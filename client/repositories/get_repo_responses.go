// Code generated by go-swagger; DO NOT EDIT.

package repositories

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

// GetRepoReader is a Reader for the GetRepo structure.
type GetRepoReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *GetRepoReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewGetRepoOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewGetRepoDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewGetRepoOK creates a GetRepoOK with default headers values
func NewGetRepoOK() *GetRepoOK {
	return &GetRepoOK{}
}

/*
GetRepoOK describes a response with status code 200, with default header values.

Repository
*/
type GetRepoOK struct {
	Payload garm_params.Repository
}

// IsSuccess returns true when this get repo o k response has a 2xx status code
func (o *GetRepoOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this get repo o k response has a 3xx status code
func (o *GetRepoOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this get repo o k response has a 4xx status code
func (o *GetRepoOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this get repo o k response has a 5xx status code
func (o *GetRepoOK) IsServerError() bool {
	return false
}

// IsCode returns true when this get repo o k response a status code equal to that given
func (o *GetRepoOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the get repo o k response
func (o *GetRepoOK) Code() int {
	return 200
}

func (o *GetRepoOK) Error() string {
	return fmt.Sprintf("[GET /repositories/{repoID}][%d] getRepoOK  %+v", 200, o.Payload)
}

func (o *GetRepoOK) String() string {
	return fmt.Sprintf("[GET /repositories/{repoID}][%d] getRepoOK  %+v", 200, o.Payload)
}

func (o *GetRepoOK) GetPayload() garm_params.Repository {
	return o.Payload
}

func (o *GetRepoOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewGetRepoDefault creates a GetRepoDefault with default headers values
func NewGetRepoDefault(code int) *GetRepoDefault {
	return &GetRepoDefault{
		_statusCode: code,
	}
}

/*
GetRepoDefault describes a response with status code -1, with default header values.

APIErrorResponse
*/
type GetRepoDefault struct {
	_statusCode int

	Payload apiserver_params.APIErrorResponse
}

// IsSuccess returns true when this get repo default response has a 2xx status code
func (o *GetRepoDefault) IsSuccess() bool {
	return o._statusCode/100 == 2
}

// IsRedirect returns true when this get repo default response has a 3xx status code
func (o *GetRepoDefault) IsRedirect() bool {
	return o._statusCode/100 == 3
}

// IsClientError returns true when this get repo default response has a 4xx status code
func (o *GetRepoDefault) IsClientError() bool {
	return o._statusCode/100 == 4
}

// IsServerError returns true when this get repo default response has a 5xx status code
func (o *GetRepoDefault) IsServerError() bool {
	return o._statusCode/100 == 5
}

// IsCode returns true when this get repo default response a status code equal to that given
func (o *GetRepoDefault) IsCode(code int) bool {
	return o._statusCode == code
}

// Code gets the status code for the get repo default response
func (o *GetRepoDefault) Code() int {
	return o._statusCode
}

func (o *GetRepoDefault) Error() string {
	return fmt.Sprintf("[GET /repositories/{repoID}][%d] GetRepo default  %+v", o._statusCode, o.Payload)
}

func (o *GetRepoDefault) String() string {
	return fmt.Sprintf("[GET /repositories/{repoID}][%d] GetRepo default  %+v", o._statusCode, o.Payload)
}

func (o *GetRepoDefault) GetPayload() apiserver_params.APIErrorResponse {
	return o.Payload
}

func (o *GetRepoDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}
