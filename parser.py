import string, sys, bz2, json
import elasticsearch as es

client = es.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])

mappings = {
    'post': {
        'properties': {
            'subreddit': {'type': 'text'},
            'author': {'type': 'text'},
            'body': {'type': 'text'},
            'created': {'type': 'date', 'format': 'epoch_second'},
            'score': {'type': 'integer'},
        }
    }
}

body = {'mappings': mappings}

try:
    client.indices.create(index=index_name, body=body)
except es.exceptions.TransportError as e:
    if e.error != 'index_already_exists_exception':
        raise

class DatasetReader(object):
    """Iterate on the objects stored in a reddit dataset."""

    def __init__(self, path):
        self.fp = bz2.open(path, mode='rt')
        self.decoder = json.JSONDecoder()

    def __iter__(self):
        return self

    def __next__(self):
        line = self.fp.readline()
        if not line:
            raise StopIteration
        return self.decoder.decode(line)

for obj in DatasetReader('RC_2015-100L.bz2'):
    try:
        post_id_str = obj['id']
        if post_id_str.startswith('tr1_'):
            post_id_str = post_idx_str[4:]
        post_id = int(post_id_str, 36)
    except ValueError:
        print('ignore post with invalid id "%s"' % (obj), file=sys.stderr)
        continue

    try:
        created_str = obj['created_utc']
        created = int(created_str)
    except ValueError:
        print('ignore post with invalid timestamp "%s"' % (created_str),
              file=sys.stderr)
        continue

    document = {
        'id': post_id,
        'subreddit': obj['subreddit'],
        'created': created,
        'score': obj['score'],
    }

    body = obj['body']
    if body != '[deleted]':
        document['body'] = body

    author = obj['author']
    if author != '[deleted]':
        document['author'] = author

    client.index(index='reddit', doc_type='post', id=post_id,
                 body=document)
