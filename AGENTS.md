# Go Tests

Use the local FrankenPHP PHP 8.5 ZTS build when running module tests:

```bash
export PHP_PREFIX="$HOME/.local/frankenphp/php-8.5-zts"
export PATH="$PHP_PREFIX/bin:$PATH"
export PHP_CONFIG="$PHP_PREFIX/bin/php-config"
export CGO_ENABLED=1
export CGO_CFLAGS="-D_GNU_SOURCE -g -O0 $($PHP_CONFIG --includes)"
export CGO_CPPFLAGS="$($PHP_CONFIG --includes)"
export CGO_LDFLAGS="-L$PHP_PREFIX/lib -Wl,-rpath,$PHP_PREFIX/lib $($PHP_CONFIG --ldflags) $($PHP_CONFIG --libs)"
go test -v ./... -tags=nobadger,nomysql,nopgx,nowatcher -mod=readonly
```
