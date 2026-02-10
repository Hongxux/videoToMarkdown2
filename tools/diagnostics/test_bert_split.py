import json
import os
import sys

# DEBUG LOGGING SETUP
def log(msg):
    with open("debug_status.txt", "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

# Clear log
with open("debug_status.txt", "w", encoding="utf-8") as f:
    f.write("Script initialized\n")

try:
    import numpy as np
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity
    log("Imports successful")
except Exception as e:
    log(f"Import Error: {e}")
    sys.exit(1)

def calculate_similarity(text1, text2, model):
    embeddings = model.encode([text1, text2])
    return cosine_similarity([embeddings[0]], [embeddings[1]])[0][0]

def main():
    try:
        json_path = r"d:\videoToMarkdownTest2\storage\99efb7c15a9121f4e29113821d5c9c73\intermediates\step6_merge_cross_output.json"
        target_batches = 5
        min_batch_size = 5  # Ensure no batch is smaller than this
        
        log(f"Loading data from {json_path}...")
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        paragraphs = data.get("output", {}).get("pure_text_script", [])
        if not paragraphs:
            log("No paragraphs found in JSON.")
            return

        total_p = len(paragraphs)
        log(f"Found {total_p} paragraphs. Target: ~{target_batches} batches.")

        log("Loading BERT model (paraphrase-multilingual-MiniLM-L12-v2)...")
        try:
            model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
            log("Model loaded.")
        except Exception as e:
            log(f"Failed to load sentence-transformers: {e}")
            return

        # 1. Calculate adjacent similarities
        log("Calculating similarity curve...")
        similarities = []
        for i in range(total_p - 1):
            t1 = paragraphs[i].get("text", "")
            t2 = paragraphs[i+1].get("text", "")
            sim = calculate_similarity(t1, t2, model)
            similarities.append((i, sim))
        
        # 2. Find best split points (Lowest K similarities)
        # We want (target_batches - 1) splits
        num_splits = target_batches - 1
        
        # Sort by similarity (ascending)
        sorted_sims = sorted(similarities, key=lambda x: x[1])
        
        final_split_indices = []
        
        log(f"Evaluating splits (Min seg: {min_batch_size})...")
        
        for idx, sim in sorted_sims:
            if len(final_split_indices) >= num_splits:
                break
                
            # Check constraints
            potential_splits = sorted(final_split_indices + [idx])
            
            valid = True
            prev_idx = -1
            for split_idx in potential_splits:
                segment_len = split_idx - prev_idx
                if segment_len < min_batch_size:
                    valid = False
                    break
                prev_idx = split_idx
                
            # Check last segment
            if (total_p - 1) - prev_idx < min_batch_size:
                valid = False
            
            if valid:
                final_split_indices.append(idx)
                log(f"  [ACCEPTED] Split after P{idx:03d} (Sim: {sim:.4f})")
            else:
                pass
                # log(f"  [SKIPPED] Split after P{idx:03d} (Sim: {sim:.4f})")

        final_split_indices.sort()
        log(f"Final Split Indices: {final_split_indices}")
        
        # 3. Generate Report
        batches = []
        start_idx = 0
        for split_at in final_split_indices:
            end_idx = split_at + 1
            batches.append(paragraphs[start_idx:end_idx])
            start_idx = end_idx
        batches.append(paragraphs[start_idx:]) # Last batch

        log(f"Generated {len(batches)} Batches")
        
        report_path = "bert_split_report_optimized.txt"
        with open(report_path, "w", encoding="utf-8") as f:
            for b_i, batch in enumerate(batches):
                f.write(f"=== Batch {b_i+1} (Size: {len(batch)}) ===\n")
                if len(batch) > 1:
                    batch_sims = []
                    for k in range(len(batch)-1):
                        s = calculate_similarity(batch[k]['text'], batch[k+1]['text'], model)
                        batch_sims.append(s)
                    avg_sim = sum(batch_sims)/len(batch_sims)
                    f.write(f"Avg Internal Similarity: {avg_sim:.4f}\n")
                
                for p in batch:
                    f.write(f"  [{p.get('paragraph_id')}] {p.get('text')}\n")
                f.write("\n")
                
        log(f"Detailed report saved to {report_path}")

    except Exception as e:
        log(f"Runtime Error: {e}")
        import traceback
        log(traceback.format_exc())

if __name__ == "__main__":
    main()
