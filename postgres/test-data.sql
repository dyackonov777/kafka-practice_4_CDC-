-- Дополнительные данные для проверки CDC после запуска коннектора
-- (операции INSERT, UPDATE и DELETE)

INSERT INTO users (name, email) VALUES ('Test User 1', 'user1@test.com');
INSERT INTO users (name, email) VALUES ('Test User 2', 'user2@test.com');

INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Notebook', 1);
INSERT INTO orders (user_id, product_name, quantity) VALUES (2, 'Keyboard', 2);
INSERT INTO orders (user_id, product_name, quantity) VALUES (3, 'Mouse', 3);

UPDATE users SET email = 'john.doe@example.com' WHERE name = 'John Doe';

UPDATE orders SET quantity = 10 WHERE product_name = 'Keyboard';

DELETE FROM orders WHERE product_name = 'Mouse';
