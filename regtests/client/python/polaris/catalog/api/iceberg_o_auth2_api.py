# coding: utf-8

"""
    Apache Iceberg REST Catalog API

    Defines the specification for the first version of the REST Catalog API. Implementations should ideally support both Iceberg table specs v1 and v2, with priority given to v2.

    The version of the OpenAPI document: 0.0.1
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501

import warnings
from pydantic import validate_call, Field, StrictFloat, StrictStr, StrictInt
from typing import Any, Dict, List, Optional, Tuple, Union
from typing_extensions import Annotated

from pydantic import Field, StrictStr, field_validator
from typing import Optional
from typing_extensions import Annotated
from polaris.catalog.models.o_auth_token_response import OAuthTokenResponse
from polaris.catalog.models.token_type import TokenType

from polaris.catalog.api_client import ApiClient, RequestSerialized
from polaris.catalog.api_response import ApiResponse
from polaris.catalog.rest import RESTResponseType


class IcebergOAuth2API:
    """NOTE: This class is auto generated by OpenAPI Generator
    Ref: https://openapi-generator.tech

    Do not edit the class manually.
    """

    def __init__(self, api_client=None) -> None:
        if api_client is None:
            api_client = ApiClient.get_default()
        self.api_client = api_client


    @validate_call
    def get_token(
        self,
        authorization: Optional[StrictStr] = None,
        grant_type: Optional[StrictStr] = None,
        scope: Optional[StrictStr] = None,
        client_id: Annotated[Optional[StrictStr], Field(description="Client ID  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.")] = None,
        client_secret: Annotated[Optional[StrictStr], Field(description="Client secret  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.")] = None,
        requested_token_type: Optional[TokenType] = None,
        subject_token: Annotated[Optional[StrictStr], Field(description="Subject token for token exchange request")] = None,
        subject_token_type: Optional[TokenType] = None,
        actor_token: Annotated[Optional[StrictStr], Field(description="Actor token for token exchange request")] = None,
        actor_token_type: Optional[TokenType] = None,
        _request_timeout: Union[
            None,
            Annotated[StrictFloat, Field(gt=0)],
            Tuple[
                Annotated[StrictFloat, Field(gt=0)],
                Annotated[StrictFloat, Field(gt=0)]
            ]
        ] = None,
        _request_auth: Optional[Dict[StrictStr, Any]] = None,
        _content_type: Optional[StrictStr] = None,
        _headers: Optional[Dict[StrictStr, Any]] = None,
        _host_index: Annotated[StrictInt, Field(ge=0, le=0)] = 0,
    ) -> OAuthTokenResponse:
        """Get a token using an OAuth2 flow

        Exchange credentials for a token using the OAuth2 client credentials flow or token exchange.  This endpoint is used for three purposes - 1. To exchange client credentials (client ID and secret) for an access token This uses the client credentials flow. 2. To exchange a client token and an identity token for a more specific access token This uses the token exchange flow. 3. To exchange an access token for one with the same claims and a refreshed expiration period This uses the token exchange flow.  For example, a catalog client may be configured with client credentials from the OAuth2 Authorization flow. This client would exchange its client ID and secret for an access token using the client credentials request with this endpoint (1). Subsequent requests would then use that access token.  Some clients may also handle sessions that have additional user context. These clients would use the token exchange flow to exchange a user token (the \"subject\" token) from the session for a more specific access token for that user, using the catalog's access token as the \"actor\" token (2). The user ID token is the \"subject\" token and can be any token type allowed by the OAuth2 token exchange flow, including a unsecured JWT token with a sub claim. This request should use the catalog's bearer token in the \"Authorization\" header.  Clients may also use the token exchange flow to refresh a token that is about to expire by sending a token exchange request (3). The request's \"subject\" token should be the expiring token. This request should use the subject token in the \"Authorization\" header.

        :param authorization:
        :type authorization: str
        :param grant_type:
        :type grant_type: str
        :param scope:
        :type scope: str
        :param client_id: Client ID  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.
        :type client_id: str
        :param client_secret: Client secret  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.
        :type client_secret: str
        :param requested_token_type:
        :type requested_token_type: TokenType
        :param subject_token: Subject token for token exchange request
        :type subject_token: str
        :param subject_token_type:
        :type subject_token_type: TokenType
        :param actor_token: Actor token for token exchange request
        :type actor_token: str
        :param actor_token_type:
        :type actor_token_type: TokenType
        :param _request_timeout: timeout setting for this request. If one
                                 number provided, it will be total request
                                 timeout. It can also be a pair (tuple) of
                                 (connection, read) timeouts.
        :type _request_timeout: int, tuple(int, int), optional
        :param _request_auth: set to override the auth_settings for an a single
                              request; this effectively ignores the
                              authentication in the spec for a single request.
        :type _request_auth: dict, optional
        :param _content_type: force content-type for the request.
        :type _content_type: str, Optional
        :param _headers: set to override the headers for a single
                         request; this effectively ignores the headers
                         in the spec for a single request.
        :type _headers: dict, optional
        :param _host_index: set to override the host_index for a single
                            request; this effectively ignores the host_index
                            in the spec for a single request.
        :type _host_index: int, optional
        :return: Returns the result object.
        """ # noqa: E501

        _param = self._get_token_serialize(
            authorization=authorization,
            grant_type=grant_type,
            scope=scope,
            client_id=client_id,
            client_secret=client_secret,
            requested_token_type=requested_token_type,
            subject_token=subject_token,
            subject_token_type=subject_token_type,
            actor_token=actor_token,
            actor_token_type=actor_token_type,
            _request_auth=_request_auth,
            _content_type=_content_type,
            _headers=_headers,
            _host_index=_host_index
        )

        _response_types_map: Dict[str, Optional[str]] = {
            '200': "OAuthTokenResponse",
            '400': "OAuthError",
            '401': "OAuthError",
            '5XX': "OAuthError",
        }
        response_data = self.api_client.call_api(
            *_param,
            _request_timeout=_request_timeout
        )
        response_data.read()
        return self.api_client.response_deserialize(
            response_data=response_data,
            response_types_map=_response_types_map,
        ).data


    @validate_call
    def get_token_with_http_info(
        self,
        authorization: Optional[StrictStr] = None,
        grant_type: Optional[StrictStr] = None,
        scope: Optional[StrictStr] = None,
        client_id: Annotated[Optional[StrictStr], Field(description="Client ID  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.")] = None,
        client_secret: Annotated[Optional[StrictStr], Field(description="Client secret  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.")] = None,
        requested_token_type: Optional[TokenType] = None,
        subject_token: Annotated[Optional[StrictStr], Field(description="Subject token for token exchange request")] = None,
        subject_token_type: Optional[TokenType] = None,
        actor_token: Annotated[Optional[StrictStr], Field(description="Actor token for token exchange request")] = None,
        actor_token_type: Optional[TokenType] = None,
        _request_timeout: Union[
            None,
            Annotated[StrictFloat, Field(gt=0)],
            Tuple[
                Annotated[StrictFloat, Field(gt=0)],
                Annotated[StrictFloat, Field(gt=0)]
            ]
        ] = None,
        _request_auth: Optional[Dict[StrictStr, Any]] = None,
        _content_type: Optional[StrictStr] = None,
        _headers: Optional[Dict[StrictStr, Any]] = None,
        _host_index: Annotated[StrictInt, Field(ge=0, le=0)] = 0,
    ) -> ApiResponse[OAuthTokenResponse]:
        """Get a token using an OAuth2 flow

        Exchange credentials for a token using the OAuth2 client credentials flow or token exchange.  This endpoint is used for three purposes - 1. To exchange client credentials (client ID and secret) for an access token This uses the client credentials flow. 2. To exchange a client token and an identity token for a more specific access token This uses the token exchange flow. 3. To exchange an access token for one with the same claims and a refreshed expiration period This uses the token exchange flow.  For example, a catalog client may be configured with client credentials from the OAuth2 Authorization flow. This client would exchange its client ID and secret for an access token using the client credentials request with this endpoint (1). Subsequent requests would then use that access token.  Some clients may also handle sessions that have additional user context. These clients would use the token exchange flow to exchange a user token (the \"subject\" token) from the session for a more specific access token for that user, using the catalog's access token as the \"actor\" token (2). The user ID token is the \"subject\" token and can be any token type allowed by the OAuth2 token exchange flow, including a unsecured JWT token with a sub claim. This request should use the catalog's bearer token in the \"Authorization\" header.  Clients may also use the token exchange flow to refresh a token that is about to expire by sending a token exchange request (3). The request's \"subject\" token should be the expiring token. This request should use the subject token in the \"Authorization\" header.

        :param authorization:
        :type authorization: str
        :param grant_type:
        :type grant_type: str
        :param scope:
        :type scope: str
        :param client_id: Client ID  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.
        :type client_id: str
        :param client_secret: Client secret  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.
        :type client_secret: str
        :param requested_token_type:
        :type requested_token_type: TokenType
        :param subject_token: Subject token for token exchange request
        :type subject_token: str
        :param subject_token_type:
        :type subject_token_type: TokenType
        :param actor_token: Actor token for token exchange request
        :type actor_token: str
        :param actor_token_type:
        :type actor_token_type: TokenType
        :param _request_timeout: timeout setting for this request. If one
                                 number provided, it will be total request
                                 timeout. It can also be a pair (tuple) of
                                 (connection, read) timeouts.
        :type _request_timeout: int, tuple(int, int), optional
        :param _request_auth: set to override the auth_settings for an a single
                              request; this effectively ignores the
                              authentication in the spec for a single request.
        :type _request_auth: dict, optional
        :param _content_type: force content-type for the request.
        :type _content_type: str, Optional
        :param _headers: set to override the headers for a single
                         request; this effectively ignores the headers
                         in the spec for a single request.
        :type _headers: dict, optional
        :param _host_index: set to override the host_index for a single
                            request; this effectively ignores the host_index
                            in the spec for a single request.
        :type _host_index: int, optional
        :return: Returns the result object.
        """ # noqa: E501

        _param = self._get_token_serialize(
            authorization=authorization,
            grant_type=grant_type,
            scope=scope,
            client_id=client_id,
            client_secret=client_secret,
            requested_token_type=requested_token_type,
            subject_token=subject_token,
            subject_token_type=subject_token_type,
            actor_token=actor_token,
            actor_token_type=actor_token_type,
            _request_auth=_request_auth,
            _content_type=_content_type,
            _headers=_headers,
            _host_index=_host_index
        )

        _response_types_map: Dict[str, Optional[str]] = {
            '200': "OAuthTokenResponse",
            '400': "OAuthError",
            '401': "OAuthError",
            '5XX': "OAuthError",
        }
        response_data = self.api_client.call_api(
            *_param,
            _request_timeout=_request_timeout
        )
        response_data.read()
        return self.api_client.response_deserialize(
            response_data=response_data,
            response_types_map=_response_types_map,
        )


    @validate_call
    def get_token_without_preload_content(
        self,
        authorization: Optional[StrictStr] = None,
        grant_type: Optional[StrictStr] = None,
        scope: Optional[StrictStr] = None,
        client_id: Annotated[Optional[StrictStr], Field(description="Client ID  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.")] = None,
        client_secret: Annotated[Optional[StrictStr], Field(description="Client secret  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.")] = None,
        requested_token_type: Optional[TokenType] = None,
        subject_token: Annotated[Optional[StrictStr], Field(description="Subject token for token exchange request")] = None,
        subject_token_type: Optional[TokenType] = None,
        actor_token: Annotated[Optional[StrictStr], Field(description="Actor token for token exchange request")] = None,
        actor_token_type: Optional[TokenType] = None,
        _request_timeout: Union[
            None,
            Annotated[StrictFloat, Field(gt=0)],
            Tuple[
                Annotated[StrictFloat, Field(gt=0)],
                Annotated[StrictFloat, Field(gt=0)]
            ]
        ] = None,
        _request_auth: Optional[Dict[StrictStr, Any]] = None,
        _content_type: Optional[StrictStr] = None,
        _headers: Optional[Dict[StrictStr, Any]] = None,
        _host_index: Annotated[StrictInt, Field(ge=0, le=0)] = 0,
    ) -> RESTResponseType:
        """Get a token using an OAuth2 flow

        Exchange credentials for a token using the OAuth2 client credentials flow or token exchange.  This endpoint is used for three purposes - 1. To exchange client credentials (client ID and secret) for an access token This uses the client credentials flow. 2. To exchange a client token and an identity token for a more specific access token This uses the token exchange flow. 3. To exchange an access token for one with the same claims and a refreshed expiration period This uses the token exchange flow.  For example, a catalog client may be configured with client credentials from the OAuth2 Authorization flow. This client would exchange its client ID and secret for an access token using the client credentials request with this endpoint (1). Subsequent requests would then use that access token.  Some clients may also handle sessions that have additional user context. These clients would use the token exchange flow to exchange a user token (the \"subject\" token) from the session for a more specific access token for that user, using the catalog's access token as the \"actor\" token (2). The user ID token is the \"subject\" token and can be any token type allowed by the OAuth2 token exchange flow, including a unsecured JWT token with a sub claim. This request should use the catalog's bearer token in the \"Authorization\" header.  Clients may also use the token exchange flow to refresh a token that is about to expire by sending a token exchange request (3). The request's \"subject\" token should be the expiring token. This request should use the subject token in the \"Authorization\" header.

        :param authorization:
        :type authorization: str
        :param grant_type:
        :type grant_type: str
        :param scope:
        :type scope: str
        :param client_id: Client ID  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.
        :type client_id: str
        :param client_secret: Client secret  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.
        :type client_secret: str
        :param requested_token_type:
        :type requested_token_type: TokenType
        :param subject_token: Subject token for token exchange request
        :type subject_token: str
        :param subject_token_type:
        :type subject_token_type: TokenType
        :param actor_token: Actor token for token exchange request
        :type actor_token: str
        :param actor_token_type:
        :type actor_token_type: TokenType
        :param _request_timeout: timeout setting for this request. If one
                                 number provided, it will be total request
                                 timeout. It can also be a pair (tuple) of
                                 (connection, read) timeouts.
        :type _request_timeout: int, tuple(int, int), optional
        :param _request_auth: set to override the auth_settings for an a single
                              request; this effectively ignores the
                              authentication in the spec for a single request.
        :type _request_auth: dict, optional
        :param _content_type: force content-type for the request.
        :type _content_type: str, Optional
        :param _headers: set to override the headers for a single
                         request; this effectively ignores the headers
                         in the spec for a single request.
        :type _headers: dict, optional
        :param _host_index: set to override the host_index for a single
                            request; this effectively ignores the host_index
                            in the spec for a single request.
        :type _host_index: int, optional
        :return: Returns the result object.
        """ # noqa: E501

        _param = self._get_token_serialize(
            authorization=authorization,
            grant_type=grant_type,
            scope=scope,
            client_id=client_id,
            client_secret=client_secret,
            requested_token_type=requested_token_type,
            subject_token=subject_token,
            subject_token_type=subject_token_type,
            actor_token=actor_token,
            actor_token_type=actor_token_type,
            _request_auth=_request_auth,
            _content_type=_content_type,
            _headers=_headers,
            _host_index=_host_index
        )

        _response_types_map: Dict[str, Optional[str]] = {
            '200': "OAuthTokenResponse",
            '400': "OAuthError",
            '401': "OAuthError",
            '5XX': "OAuthError",
        }
        response_data = self.api_client.call_api(
            *_param,
            _request_timeout=_request_timeout
        )
        return response_data.response


    def _get_token_serialize(
        self,
        authorization,
        grant_type,
        scope,
        client_id,
        client_secret,
        requested_token_type,
        subject_token,
        subject_token_type,
        actor_token,
        actor_token_type,
        _request_auth,
        _content_type,
        _headers,
        _host_index,
    ) -> RequestSerialized:

        _host = None

        _collection_formats: Dict[str, str] = {
        }

        _path_params: Dict[str, str] = {}
        _query_params: List[Tuple[str, str]] = []
        _header_params: Dict[str, Optional[str]] = _headers or {}
        _form_params: List[Tuple[str, str]] = []
        _files: Dict[str, Union[str, bytes]] = {}
        _body_params: Optional[bytes] = None

        # process the path parameters
        # process the query parameters
        # process the header parameters
        if authorization is not None:
            _header_params['Authorization'] = authorization
        # process the form parameters
        if grant_type is not None:
            _form_params.append(('grant_type', grant_type))
        if scope is not None:
            _form_params.append(('scope', scope))
        if client_id is not None:
            _form_params.append(('client_id', client_id))
        if client_secret is not None:
            _form_params.append(('client_secret', client_secret))
        if requested_token_type is not None:
            _form_params.append(('requested_token_type', requested_token_type))
        if subject_token is not None:
            _form_params.append(('subject_token', subject_token))
        if subject_token_type is not None:
            _form_params.append(('subject_token_type', subject_token_type))
        if actor_token is not None:
            _form_params.append(('actor_token', actor_token))
        if actor_token_type is not None:
            _form_params.append(('actor_token_type', actor_token_type))
        # process the body parameter


        # set the HTTP header `Accept`
        if 'Accept' not in _header_params:
            _header_params['Accept'] = self.api_client.select_header_accept(
                [
                    'application/json'
                ]
            )

        # set the HTTP header `Content-Type`
        if _content_type:
            _header_params['Content-Type'] = _content_type
        else:
            _default_content_type = (
                self.api_client.select_header_content_type(
                    [
                        'application/x-www-form-urlencoded'
                    ]
                )
            )
            if _default_content_type is not None:
                _header_params['Content-Type'] = _default_content_type

        # authentication setting
        _auth_settings: List[str] = [
            'BearerAuth'
        ]

        return self.api_client.param_serialize(
            method='POST',
            resource_path='/v1/oauth/tokens',
            path_params=_path_params,
            query_params=_query_params,
            header_params=_header_params,
            body=_body_params,
            post_params=_form_params,
            files=_files,
            auth_settings=_auth_settings,
            collection_formats=_collection_formats,
            _host=_host,
            _request_auth=_request_auth
        )


