# Tiny-driver

Yet another PostgreSQL driver.

The driver was written for educational purposes for my article on the
[Medium](https://medium.com/scum-gazeta/writing-your-own-postgresql-driver-ebd5fd6d187d)

A minimum of functionality has been implemented which should be enough to explain the principle of work.

Do not use in production  :skull:

Was inspired by:
* https://github.com/uptrace/bun
* https://github.com/lib/pq

### Example
```go
import (
    "database/sql"
    _ "github.com/ihippik/tiny-driver"
)

func main() {
    dsn := "postgres://postgres:postgrespw@localhost:55000/postgres?sslmode=disable"
    db, err := sql.Open("tiny", dsn)
}
```
