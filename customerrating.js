const express = require("express");
const mysql = require("mysql");

const db = mysql.createConnection({
  host: "localhost",
  user: "root",
  database: "node_mysql",
});

db.connect((err) => {
  if (err) throw err;
  console.log("Connected to MySQL Server!");
});

const app = express();

app.listen(4000, () => console.log("Server running on port 4000"));

app.get("/h1", (req, res) => {
  // H1 Risk Customers
  const sql = `select * from Customer_Info c, Account_Info a where c.customer_key = a.customer_key and a.account_key in (select account_key from Transaction where country_code in (select entity_key from Countries_info) group by MONTH(transfer_date) having count(MONTH(transfer_date)) > 10)`;

  db.query(sql, (err, rows) => {
    if (err) throw err;
    res.send(rows);
  });
});

app.get("/h2", (req, res) => {
  // H2 Risk Customers
  const sql = `select * from Customer_Info c, Account_Info a where c.customer_key = a.customer_key and a.account_key in (select account_key from Transaction where transaction_type = 'INN' group by MONTH(transfer_date) having sum(transfer_amt) > 1000)`;

  db.query(sql, (err, rows) => {
    if (err) throw err;
    res.send(rows);
  });
});

app.get("/h3", (req, res) => {
  // H3 Risk Customers
  const sql = `select * from Customer_Info c, Account_Info a where c.customer_key = a.customer_key and a.account_key in (select account_key from Transaction where transaction_type = 'OUT' group by MONTH(transfer_date) having sum(transfer_amt) > 800)`;

  db.query(sql, (err, rows) => {
    if (err) throw err;
    res.send(rows);
  });
});

app.get("/h4", (req, res) => {
  // H4 Risk Customers
  const sql = `select * from Customer_Info c, Account_Info a where c.customer_key = a.customer_key and a.account_key in (select account_key from Transaction group by transfer_date having count(transfer_date) > 20)`;

  db.query(sql, (err, rows) => {
    if (err) throw err;
    res.send(rows);
  });
});

app.get("/m1", (req, res) => {
  // M1 Risk Customers
  const sql = `select * from Customer_Info c, Account_Info a where c.customer_key = a.customer_key and a.account_key in (select account_key from Transaction group by MONTH(transfer_date) having count(MONTH(transfer_date)) > 6)`;

  db.query(sql, (err, rows) => {
    if (err) throw err;
    res.send(rows);
  });
});

app.get("/m2", (req, res) => {
  // M2 Risk Customers
  const sql = `select * from Customer_Info c, Account_Info a where c.customer_key = a.customer_key and a.account_key in (select account_key from Transaction where transaction_type = 'INN' group by MONTH(transfer_date) having sum(transfer_amt) > 600 and sum(transfer_amt) < 1000)`;

  db.query(sql, (err, rows) => {
    if (err) throw err;
    res.send(rows);
  });
});

app.get("/m3", (req, res) => {
  // M3 Risk Customers
  const sql = `select * from Customer_Info c, Account_Info a where c.customer_key = a.customer_key and a.account_key in (select account_key from Transaction where transaction_type = 'OUT' group by MONTH(transfer_date) having sum(transfer_amt) > 500 and sum(transfer_amt) < 800)`;

  db.query(sql, (err, rows) => {
    if (err) throw err;
    res.send(rows);
  });
});

app.get("/m4", (req, res) => {
  // M4 Risk Customers
  const sql = `select * from Customer_Info c, Account_Info a where c.customer_key = a.customer_key and a.account_key in (select account_key from Transaction group by transfer_date having count(transfer_date) > 10 and count(transfer_date) < 20)`;

  db.query(sql, (err, rows) => {
    if (err) throw err;
    res.send(rows);
  });
});

app.get("/l1", (req, res) => {
  // L1 Risk Customers
  const sql = `select * from Customer_Info c, Account_Info a where c.customer_key = a.customer_key and a.account_key in (select account_key from Transaction where country_code not in (select entity_key from Countries_info) group by MONTH(transfer_date) having count(MONTH(transfer_date)) > 10)`;

  db.query(sql, (err, rows) => {
    if (err) throw err;
    res.send(rows);
  });
});

app.get("/l2", (req, res) => {
  // L2 Risk Customers
  const sql = `select * from Customer_Info c, Account_Info a where c.customer_key = a.customer_key and a.account_key in (select account_key from Transaction where transaction_type = 'INN' group by MONTH(transfer_date) having sum(transfer_amt) < 600)`;

  db.query(sql, (err, rows) => {
    if (err) throw err;
    res.send(rows);
  });
});

app.get("/l3", (req, res) => {
  // L3 Risk Customers
  const sql = `select * from Customer_Info c, Account_Info a where c.customer_key = a.customer_key and a.account_key in (select account_key from Transaction where transaction_type = 'OUT' group by MONTH(transfer_date) having sum(transfer_amt) < 500)`;

  db.query(sql, (err, rows) => {
    if (err) throw err;
    res.send(rows);
  });
});

app.get("/l4", (req, res) => {
  // L4 Risk Customers
  const sql = `select * from Customer_Info c, Account_Info a where c.customer_key = a.customer_key and a.account_key in (select account_key from Transaction group by transfer_date having count(transfer_date) < 10)`;

  db.query(sql, (err, rows) => {
    if (err) throw err;
    res.send(rows);
  });
});