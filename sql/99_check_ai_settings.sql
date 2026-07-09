SELECT
  name,
  value,
  changed,
  description
FROM system.settings
WHERE name = 'allow_experimental_ai_functions'
   OR name LIKE 'ai_function_%'
ORDER BY name;
