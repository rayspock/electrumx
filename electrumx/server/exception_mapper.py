import json

from aiohttp import web, web_middlewares


def json_error(status_code: int, exception: Exception) -> web.Response:
    return web.Response(
        status=status_code,
        body=json.dumps({
            'error': exception.__class__.__name__,
            'detail': str(exception)
        }).encode('utf-8'),
        content_type='application/json')


def error_middleware(self) -> web_middlewares:
    async def factory(app: web.Application, handler):
        async def middleware_handler(request):
            try:
                response = await handler(request)
                if response.status == 404 or response.status == 400:
                    return json_error(response.status, Exception(response.text))
                return response
            except web.HTTPException as ex:
                if ex.status == 404:
                    return json_error(ex.status, ex)
                raise
            except Exception as e:
                self.logger.warning('Request {} has failed with exception: {}'.format(request, repr(e)))
                return json_error(500, e)

        return middleware_handler
    return factory
