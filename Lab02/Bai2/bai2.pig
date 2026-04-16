raw_data = LOAD '/user/hoag/ds200_lab02/input/hotel-review.csv'
USING PigStorage(';')
AS (
    id:chararray,
    review:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
);

stopwords = LOAD '/user/hoag/ds200_lab02/input/stopwords.txt'
USING PigStorage()
AS (stopword:chararray);

reviews_lower = FOREACH raw_data GENERATE
    id,
    LOWER(review) AS review_lower,
    category,
    aspect,
    sentiment;

tokenized = FOREACH reviews_lower GENERATE
    id,
    FLATTEN(TOKENIZE(review_lower)) AS word,
    category,
    aspect,
    sentiment;

filtered_words = FILTER tokenized BY word IS NOT NULL AND SIZE(word) > 0;

joined_stop = JOIN filtered_words BY word LEFT OUTER, stopwords BY stopword;
clean_words = FILTER joined_stop BY stopwords::stopword IS NULL;

clean_result = FOREACH clean_words GENERATE
    filtered_words::id AS id,
    filtered_words::word AS word,
    filtered_words::category AS category,
    filtered_words::aspect AS aspect,
    filtered_words::sentiment AS sentiment;

group_word = GROUP clean_result BY word;

word_freq = FOREACH group_word GENERATE
    group AS word,
    COUNT(clean_result) AS freq;

word_freq_500 = FILTER word_freq BY freq > 500;
word_freq_500_sorted = ORDER word_freq_500 BY freq DESC;

review_category_pairs = FOREACH raw_data GENERATE id, category;
review_category_distinct = DISTINCT review_category_pairs;
group_category = GROUP review_category_distinct BY category;

count_by_category = FOREACH group_category GENERATE
    group AS category,
    COUNT(review_category_distinct) AS num_reviews;

count_by_category_sorted = ORDER count_by_category BY num_reviews DESC;

review_aspect_pairs = FOREACH raw_data GENERATE id, aspect;
review_aspect_distinct = DISTINCT review_aspect_pairs;
group_aspect = GROUP review_aspect_distinct BY aspect;

count_by_aspect = FOREACH group_aspect GENERATE
    group AS aspect,
    COUNT(review_aspect_distinct) AS num_reviews;

count_by_aspect_sorted = ORDER count_by_aspect BY num_reviews DESC;

STORE word_freq_500_sorted INTO '/user/hoag/ds200_lab02/output/bai2_words' USING PigStorage(';');
STORE count_by_category_sorted INTO '/user/hoag/ds200_lab02/output/bai2_category' USING PigStorage(';');
STORE count_by_aspect_sorted INTO '/user/hoag/ds200_lab02/output/bai2_aspect' USING PigStorage(';');
