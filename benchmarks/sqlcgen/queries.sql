-- name: InsertUser :one
INSERT INTO users(name, age)
VALUES ($1, $2) RETURNING id;

-- name: GetUser :one
SELECT id, name, age FROM users
OFFSET $1 LIMIT 1;

-- name: List10Users :many
SELECT id, name, age FROM users
OFFSET $1 LIMIT 10;
