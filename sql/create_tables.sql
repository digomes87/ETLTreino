CREATE TABLE IF NOT EXISTS public.transformed_transactions (
    id TEXT NOT NULL,
    product TEXT NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    currency TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    status TEXT NOT NULL,
    date DATE,
    month INTEGER
);

