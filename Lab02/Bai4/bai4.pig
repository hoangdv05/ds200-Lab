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

positive_words = FILTER clean_result BY sentiment == 'positive';
group_pos = GROUP positive_words BY (category, word);

count_pos = FOREACH group_pos GENERATE
    group.category AS category,
    group.word AS word,
    COUNT(positive_words) AS freq;

group_pos_by_cat = GROUP count_pos BY category;

top5_pos_each_cat = FOREACH group_pos_by_cat {
    sorted_pos = ORDER count_pos BY freq DESC;
    top5_pos = LIMIT sorted_pos 5;
    GENERATE FLATTEN(top5_pos);
};

negative_words = FILTER clean_result BY sentiment == 'negative';
group_neg = GROUP negative_words BY (category, word);

count_neg = FOREACH group_neg GENERATE
    group.category AS category,
    group.word AS word,
    COUNT(negative_words) AS freq;

group_neg_by_cat = GROUP count_neg BY category;

top5_neg_each_cat = FOREACH group_neg_by_cat {
    sorted_neg = ORDER count_neg BY freq DESC;
    top5_neg = LIMIT sorted_neg 5;
    GENERATE FLATTEN(top5_neg);
};

STORE top5_pos_each_cat INTO '/user/hoag/ds200_lab02/output/bai4_top5_positive_words_by_category' USING PigStorage(';');
STORE top5_neg_each_cat INTO '/user/hoag/ds200_lab02/output/bai4_top5_negative_words_by_category' USING PigStorage(';');
