SELECT
    {{ model }}.message_pk,
    {{ model }}.message_length
FROM
    {{ model }}
WHERE
    {{ model }}.message_length < 0