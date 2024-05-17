from kedro.pipeline import Pipeline, node
from functools import partial, update_wrapper
from .nodes_ksei import (
    get_stock_info, 
    update_stock_listed,
    )
from .nodes_market import (
#     update_date_list,
    update_index_values,
#     get_market_data,
#     select,
#     calculate_daily_return,
    )


def create_pipeline(**kwargs):
    # select_adj_close = partial(select, column='adj_close')
    # update_wrapper(select_adj_close, select)
    return Pipeline(
        [
            node(
                get_stock_info,
                None,
                "stock_info",
                name="stock_info",
            ),
            node(
                update_index_values,
                None,
                "index_value_jci",
                name="index_value_jci",
            ),
            # node(
            #     update_date_list,
            #     'index_value_jci',
            #     "date_list",
            #     name="date_list",
            # ),
            # node(
            #     update_stock_listed,
            #     ['listed', 'delisted', 'excluded', 'date_list'],
            #     "stock_listed",
            #     name="stock_listed",
            # ),
            # node(
            #     get_market_data,
            #     'stock_listed',
            #     "raw_market_data",
            #     name="raw_market_data",
            # ),
            # node(
            #     select_adj_close, 
            #     'raw_market_data',
            #     "adj_close",
            #     name="adj_close",
            # ),
            # node(
            #     calculate_daily_return, 
            #     'adj_close',
            #     "daily_return",
            #     name="daily_return",
            # ),
            # node(
            #     get_intraday_data,
            #     ['date_list', 'listed', 'delisted'],
            #     ['intraday_stock', 'intraday_jci'],
            #     name='intraday'
            # ),
            # node(
            #     get_ca_data,
            #     None,
            #     'corporate_action_raw',
            #     name='corporate_action_raw'
            # )
        ]
    )