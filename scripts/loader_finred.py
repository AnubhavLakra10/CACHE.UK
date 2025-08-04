import os
from dataclasses import dataclass
from typing import List


@dataclass
class RelationExample:
    tokens: List[str]
    head_text: str
    tail_text: str
    relation: str
    sent_id: int

def load_finred_split(split: str, base_path: str = 'data/raw/finred') -> List[RelationExample]:
    """
    Load FinRED split: 'train', 'dev', or 'test'.
    Expects:
      - {split}.sent      (one sentence per line)
      - {split}.tup       (one line per sentence, '|'-sep triples "head ; tail ; rel")
      - relations.txt     (list of valid relation names, one per line)
    """
    sent_path = os.path.join(base_path, f'{split}.sent')
    tup_path  = os.path.join(base_path, f'{split}.tup')
    rels_path = os.path.join(base_path, 'relations.txt')

    # sanity checks
    assert os.path.exists(sent_path), f"Missing {sent_path}"
    assert os.path.exists(tup_path),  f"Missing {tup_path}"
    assert os.path.exists(rels_path), f"Missing {rels_path}"

    # load valid relation names (so you can optionally filter out unknowns)
    with open(rels_path, 'r', encoding='utf-8') as f:
        valid_rels = {line.strip() for line in f if line.strip()}

    # load sentences
    with open(sent_path, 'r', encoding='utf-8') as f:
        sentences = [line.rstrip('\n') for line in f]
    
    examples: List[RelationExample] = []
    with open(tup_path, 'r', encoding='utf-8') as f:
        for sid, line in enumerate(f):
            sent = sentences[sid]
            tokens = sent.split()

            line = line.strip()
            if not line:
                continue  # no relations for this sentence

            # each sentence may have multiple triples separated by '|'
            triples = [tr.strip() for tr in line.split('|')]
            for triple in triples:
                parts = [p.strip() for p in triple.split(';')]
                if len(parts) != 3:
                    # malformed triple
                    continue
                head_ent, tail_ent, rel = parts
                if rel not in valid_rels:
                    # skip anything not in the official relations.txt
                    continue

                examples.append(RelationExample(
                    tokens=tokens,
                    head_text=head_ent,
                    tail_text=tail_ent,
                    relation=rel,
                    sent_id=sid
                ))

    return examples

# quick CLI smoke test
if __name__ == '__main__':
    for split in ('train','dev','test'):
        exs = load_finred_split(split)
        print(f"{split}: {len(exs)} examples; sample ->", exs[:3])
