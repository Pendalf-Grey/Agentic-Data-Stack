INSERT INTO analytics.llm_investigations
(
  investigation_id,
  user_question,
  time_from,
  time_to,
  source_name,
  index_like,
  status
)
VALUES
(
  {investigation_id:String},
  {user_question:String},
  toDateTime64({time_from:String}, 3, 'UTC'),
  toDateTime64({time_to:String}, 3, 'UTC'),
  {source_name:String},
  {index_like:String},
  'running'
);
