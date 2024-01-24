from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModel
import torch
import torch.nn.functional as norm_func
import numpy as np


def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[0]  # First element of model_output contains all token embeddings
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)


class Model:
    def __init__(self, model_name, dims=0, max_length=0, similarity=""):
        self.model = SentenceTransformer(f"sentence-transformers/{model_name}")
        self.model_name = model_name
        self.max_length = max_length
        self.dims = dims
        self.similarity = similarity

    def encode(self, string):
        return self.model.encode(string)

    """
    Handling long text to produce embeddings
    Reference: https://towardsdatascience.com/how-to-apply-transformers-to-any-length-of-text-a5601410af7f
    """

    def encode_plus(self, sentence):
        tokenizer = AutoTokenizer.from_pretrained(f"sentence-transformers/{self.model_name}")
        model = AutoModel.from_pretrained(f"sentence-transformers/{self.model_name}")
        tokens = tokenizer.encode_plus(sentence, return_tensors='pt', add_special_tokens=False)
        max_token_length = model.config.max_position_embeddings - 2
        input_ids = tokens['input_ids'][0].split(max_token_length - 2)
        masks = tokens['attention_mask'][0].split(max_token_length - 2)

        input_id_chunks = []
        mask_chunks = []
        for i in range(len(input_ids)):
            # get required padding length
            pad_len = max_token_length - input_ids[i].shape[0]
            # check if tensor length satisfies required chunk size
            if pad_len > 0:
                # if padding length is more than 0, we must add padding
                input_id_chunks.append(torch.cat([
                    input_ids[i], torch.Tensor([0] * pad_len)
                ]))
                mask_chunks.append(torch.cat([
                    masks[i], torch.Tensor([0] * pad_len)
                ]))

        input_ids = torch.stack(input_id_chunks)
        attention_mask = torch.stack(mask_chunks)

        input_dict = {
            'input_ids': input_ids.long(),
            'attention_mask': attention_mask.int()
        }
        with torch.no_grad():
            model_output = model(**input_dict)

        sentence_embeddings = mean_pooling(model_output, input_dict['attention_mask'])
        sentence_embeddings = norm_func.normalize(sentence_embeddings, p=2, dim=1)
        return sentence_embeddings.mean(dim=0).numpy()
