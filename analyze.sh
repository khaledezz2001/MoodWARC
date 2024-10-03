#!/bin/bash
#SBATCH --job-name=moodwarc
#SBATCH --output=analyze_output.log
#SBATCH --error=analyze_error.log
#SBATCH --partition=gpu
#SBATCH --account=g.ba021
#SBATCH --gres=gpu:1
#SBATCH --ntasks=1
#SBATCH --time=4-00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4

CUDA_VISIBLE_DEVICES=0 python analyze.py --warc_dir /lfs01/datasets/commoncrawl/2023-2024/data.commoncrawl.org/crawl-data/CC-NEWS/2023 --db_name cc2023.db &

CUDA_VISIBLE_DEVICES=1 python analyze.py --warc_dir /lfs01/datasets/commoncrawl/2023-2024/data.commoncrawl.org/crawl-data/CC-NEWS/2024 --db_name cc2024.db &

wait
