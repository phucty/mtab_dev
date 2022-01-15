from api import m_f
from api.annotator import m_semantic, m_input
import m_config as cf
from api.utilities import m_io as iw


def eval_semtab_2020_cea(ano_cea, c_round=4):
    dir_gt_cea = f"{cf.DIR_SEMTAB_2}/GT/CEA/CEA_Round{c_round}_gt.csv"
    gt_cea, n_cea = m_input.parse_target_cea(dir_gt_cea)
    annotated_cells = set()
    correct_cells = 0
    for table_id, r_i, c_i, cea_wd in ano_cea:

        tar_cell = f"{table_id}|{r_i}|{c_i}"
        if tar_cell in annotated_cells:
            raise Exception("Duplicate cells in the submission file")
        annotated_cells.add(tar_cell)

        gt_cea_item = gt_cea[table_id].is_tar(r_i, c_i)

        if gt_cea_item.lower() == cea_wd.lower():
            correct_cells += 1

    precision = correct_cells * 1. / len(annotated_cells) if len(annotated_cells) > 0 else 0.0
    recall = correct_cells * 1. / n_cea
    f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
    print(f"F1:{f1:4f} | P: {precision:.4f} | R: {recall:.4f} | Correct: {correct_cells} | Submit: {len(annotated_cells)} | GT: {n_cea}")


def eval_semtab_2020_cta(ano_cta, c_round=4):
    dir_gt_cta = f"{cf.DIR_SEMTAB_2}/Round{c_round}/GT/CTA/CTA_Round{c_round}_gt.csv"
    gt_cta, n_cta = m_input.parse_target_cta(dir_gt_cta)


def eval_semtab_2020_cpa(ano_cpa, c_round=4):
    dir_gt_cpa = f"{cf.DIR_SEMTAB_2}/Round{c_round}/GT/CPA/CPA_Round{c_round}_gt.csv"
    gt_cpa, n_cpa = m_input.parse_target_cpa(dir_gt_cpa)


def eval_semtab_2020(ano_cea, ano_cta, ano_cpa, c_round=4):
    eval_semtab_2020_cea(ano_cea, c_round)
    eval_semtab_2020_cta(ano_cta, c_round)
    eval_semtab_2020_cpa(ano_cpa, c_round)


def run_wd_annotation(c_round=4):
    dir_tables = f"{cf.DIR_SEMTAB_2}/Round{c_round}/tables"
    dir_tar_cea = f"{cf.DIR_SEMTAB_2}/Round{c_round}/CEA_Round{c_round}_Targets.csv"
    dir_tar_cta = f"{cf.DIR_SEMTAB_2}/Round{c_round}/CTA_Round{c_round}_Targets.csv"
    dir_tar_cpa = f"{cf.DIR_SEMTAB_2}/Round{c_round}/CPA_Round{c_round}_Targets.csv"

    ano_cea, ano_cta, ano_cpa = m_semantic.annotate(dir_tables, dir_tar_cea, dir_tar_cta, dir_tar_cpa)

    # eval_semtab_2020(ano_cea, ano_cta, ano_cpa, c_round=4)
    eval_semtab_2020_cea(ano_cea, c_round)


if __name__ == '__main__':
    # iw.prepare_input_tables(cf.DIR_SAMPLE_ZIP)
    m_f.init()
    run_wd_annotation(c_round=4)
