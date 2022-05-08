package repo

import "time"

type User struct {
	ID        int       `ksql:"id"`
	Name      string    `ksql:"name"`
	Email     string    `ksql:"email"`
	CreatedAt time.Time `ksql:"created_at"`
}
