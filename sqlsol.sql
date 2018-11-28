USE  `guest_challange`;
CREATE TABLE new_payments_data  LIKE payments_data;
insert into new_payments_data  
SELECT id,DATE_FORMAT(created_at,'%Y-%m-%d') as dates,user_id,amount from payments_data v;

CREATE TABLE IF NOT EXISTS payment_cust_freq (
    id INT,
    created_at Date,
    user_id int,
    amount int,
    diff int,
    old int,
    PRIMARY KEY (id));

insert into payment_cust_freq
 SELECT v.*,
       ( Datediff(created_at, ( SELECT Min(created_at) FROM new_payments_data
         WHERE
         user_id = v.user_id)) + 1 ) AS diff, 
         (Datediff(CURRENT_DATE(), ( SELECT
Min(created_at) FROM new_payments_data WHERE user_id = v.user_id))+1) as old
FROM   new_payments_data v 
ORDER  BY user_id,diff; 

create table dump (days int,ARPC double);
DELIMITER //
CREATE PROCEDURE create_value()
Begin 
  Declare myvar double;
  Declare x int default 1;
  while (x < 366) do
	SELECT sum(amount)/count(DISTINCT(user_id)) INTO   myvar frOM   payment_cust_freq v	WHERE  old >=x AND    diff <= x;
	insert into dump(days,ARPC) values(X,myvar);
    set x= x+1;
END WHILE;
end 
//
DELIMITER ;
call create_value();

select * from dump
