SELECT
    message_pk,
    message_length
FROM
    {{ ref('fct_messages') }}
WHERE
    message_length < 0