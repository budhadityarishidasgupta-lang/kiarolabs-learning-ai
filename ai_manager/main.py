from ai_manager.jobs.math_job import run_math_lane
from ai_manager.jobs.spelling_job import run_spelling_lane
from ai_manager.jobs.synonym_job import run_synonym_lane


def main():
    run_synonym_lane()
    run_spelling_lane()
    run_math_lane()


if __name__ == "__main__":
    main()
