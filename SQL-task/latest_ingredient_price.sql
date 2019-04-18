CREATE VIEW recent_ingredient_prices
AS
  SELECT temp_table.ingredient_name,
         temp_table.price
  FROM   (SELECT price,
                 ingredient_name,
                 ingredient_two.name_ts_two,
                 Concat(ingredient_name, Cast(time_stamp AS STRING)) AS tsone
          FROM   ingredients
                 JOIN (SELECT Concat(ingredient_name, Cast(
                              Max(time_stamp) AS STRING)) AS
                              name_ts_two
                       FROM   ingredients
                       WHERE  deleted = 0
                       GROUP  BY ingredient_name) AS ingredient_two
                   ON ( ingredient_two.name_ts_two =
                        Concat(ingredient_name, Cast(
                        time_stamp AS STRING
                                                )) ))
         temp_table;  
