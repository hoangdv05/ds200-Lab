raw_data = LOAD '/user/hoag/ds200_lab02/input/hotel-review.csv'
USING PigStorage(';')
AS (
    id:chararray,
    review:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
);

review_aspect_sentiment = FOREACH raw_data GENERATE id, aspect, sentiment;
distinct_pairs = DISTINCT review_aspect_sentiment;

negative_only = FILTER distinct_pairs BY sentiment == 'negative';
group_neg = GROUP negative_only BY aspect;
count_neg = FOREACH group_neg GENERATE
    group AS aspect,
    COUNT(negative_only) AS negative_count;

all_neg = GROUP count_neg ALL;
max_neg = FOREACH all_neg GENERATE MAX(count_neg.negative_count) AS max_count;
top_negative_join = JOIN count_neg BY negative_count, max_neg BY max_count;
top_negative = FOREACH top_negative_join GENERATE
    count_neg::aspect AS aspect,
    count_neg::negative_count AS negative_count;

positive_only = FILTER distinct_pairs BY sentiment == 'positive';
group_pos = GROUP positive_only BY aspect;
count_pos = FOREACH group_pos GENERATE
    group AS aspect,
    COUNT(positive_only) AS positive_count;

all_pos = GROUP count_pos ALL;
max_pos = FOREACH all_pos GENERATE MAX(count_pos.positive_count) AS max_count;
top_positive_join = JOIN count_pos BY positive_count, max_pos BY max_count;
top_positive = FOREACH top_positive_join GENERATE
    count_pos::aspect AS aspect,
    count_pos::positive_count AS positive_count;

STORE top_negative INTO '/user/hoag/ds200_lab02/output/bai3_top_negative' USING PigStorage(';');
STORE top_positive INTO '/user/hoag/ds200_lab02/output/bai3_top_positive' USING PigStorage(';');
