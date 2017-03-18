.print Question 1 -- mercy
SELECT d1.drug_name FROM drugs d1, patients p1, medications m1       
WHERE p1.hcno = m1.hcno and m1.drug_name = d1.drug_name AND d1.category = 'anti-inflammatory' AND p1.address LIKE '%Edmonton%';

.print Question 2 -- mercy
SELECT DISTINCT s1.sym_name FROM patients p1, patients p2, symptoms s1, symptoms s2
WHERE s2.hcno <> s1.hcno
AND s2.sym_name = s1.sym_name
AND s1.hcno = p1.hcno
AND s2.hcno = p2.hcno
AND p1.address LIKE '%Edmonton%' AND p2.address LIKE '%Edmonton%'
EXCEPT SELECT s1.sym_name FROM symptoms s1, patients p3 WHERE s1.hcno = p3.hcno AND p3.address LIKE '%Calgary%';
	
.print Question 3 -- mercy
SELECT sym_name FROM symptoms s, patients p, medications m
WHERE m.drug_name = 'niacin' AND m.hcno = s.hcno
AND m.hcno = p.hcno
AND s.hcno = p.hcno
AND (SELECT date(s.obs_date)) > (SELECT date(m.mdate))
AND (SELECT date(s.obs_date)) < (SELECT date(m.mdate,'+5 day'));

.print Question 4 -- mercy
-- groups items by area code however does not format results properly
SELECT name, emg_phone  FROM patients p, medications m
WHERE p.hcno = m.hcno AND m.drug_name = 'niacin' AND m.amount > 200 AND m.days > 20
GROUP BY emg_phone
HAVING substr(p.emg_phone,1,3);

.print Question 5 -- mercy
SELECT p1.hcno, m1.drug_name
FROM patients p1, medications m1
WHERE p1.hcno = m1.hcno
AND m1.amount > 2*(SELECT AVG(m2.amount) FROM patients p2, medications m2 WHERE p2.hcno = m2.hcno AND 
m1.drug_name = m2.drug_name and p1.age_group = p2.age_group);

.print Question 6 -- mercy
-- could not come to a working solution

.print Question 7 -- mercy
SELECT m1.drug_name, AVG(m1.amount), SUM(m1.amount*m1.days)
FROM symptoms s1, medications m1
WHERE s1.hcno = m1.hcno
AND s1.sym_name = 'headache'
AND (SELECT date(m1.mdate)) = (SELECT date(s1.obs_date))
AND NOT EXISTS (SELECT s2.sym_name FROM symptoms s2 WHERE s1.hcno = s2.hcno AND (SELECT date(s1.obs_date)) = (SELECT date(s2.obs_date)) AND s1.sym_name <> s2.sym_name)
AND m1.drug_name IN (SELECT drug_name FROM medications m GROUP BY m.drug_name HAVING AVG(m.days) > 3)
GROUP BY m1.drug_name;

.print Question 8 -- mercy
SELECT r.hcno
FROM reportedallergies r
WHERE r.drug_name IN (SELECT r1.drug_name FROM reportedallergies r1 WHERE r1.hcno = '23769' AND r1.hcno <> r.hcno)
GROUP BY r.hcno
HAVING COUNT(r.drug_name) = (SELECT COUNT(r1.drug_name) FROM reportedallergies r1 WHERE r1.hcno = '23769'); 

.print Question 9 -- mercy
CREATE VIEW allergies
AS SELECT p.hcno, d.drug_name 
FROM patients p, drugs d, reportedallergies r, inferredallergies i
WHERE p.hcno = r.hcno;

.print Question 10 -- mercy
SELECT d.drug_name
FROM drugs d
WHERE d.category = 'anti-inflammatory'
AND d.drug_name NOT IN (SELECT r.drug_name FROM reportedallergies r WHERE r.hcno = '23769')
AND d.drug_name NOT IN (SELECT i.canbe_alg FROM reportedallergies r, inferredallergies i WHERE r.hcno = '23769'AND i.alg = r.drug_name);
