SELECT 
  restaurant_name, 
  late_rate, 
  gmv 
FROM `project.dataset.gold_layer` 
WHERE late_rate > 0.3 
ORDER BY gmv DESC;