import elasticsearch


def search_chapters(query, field, out_fields, exclude_fields, es_client):
    index = 'chapters'

    if field == 'keyword':
        search_query = {
            "query": {
                "query_string": {
                    "query": query
                }
            }
        }
    else:
        search_query = {
            "query": {
                "match": {
                    field: {
                        "query": query
                    }
                }
            }
        }

    response = es_client.search(index=index, body=search_query,
                                source=out_fields, size=100)

    all_hits = response['hits']['hits']

    chapters = []
    for _, chapter in enumerate(all_hits):
        result = chapter['_source']
        if result['summary']:
            if len(result['summary']) > 400:
                result['summary'] = result['summary'][0:400]
            chapters.append(result)

    return chapters


def get_chapter(chapter_id, es_client):
    try:
        return es_client.get(index='chapters', id=chapter_id)['_source']
    except elasticsearch.NotFoundError:
        return []
