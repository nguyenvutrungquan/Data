import pandas as pd, pickle
# noinspection PyUnresolvedReferences
from plotly import express as px
from bokeh.util.browser import view
from dc_server.lazy_core import maybe_create_dir
# noinspection PyUnresolvedReferences
import plotly
# noinspection PyUnresolvedReferences
from dc_server.lazy_core import mmap, tik, tok
# noinspection PyUnresolvedReferences
import numpy as np
# noinspection PyUnresolvedReferences
from plotly import express as px
from datetime import datetime as dt
from time import time
from redis import StrictRedis
from dc_server.adapters.pickle_adapter_ssi_ws import PICKLED_WS_ADAPTERS
from dc_server.sotola import FRAME


class Utilities:
    @staticmethod
    def gen_time_to_x_lookup_tables():
        from dc_server.sotola import FRAME
        MORNING_START = '09:00:00'
        MORNING_END = '11:30:59'
        AFTERNOON_START = '13:00:00'
        AFTERNOON_END = '14:45:59'

        MORNING_START_X = FRAME.timeToIndex[MORNING_START]
        # noinspection PyUnusedLocal
        MORNING_END_X = FRAME.timeToIndex[MORNING_END]

        # noinspection PyUnusedLocal
        AFTERNOON_START_X = FRAME.timeToIndex[AFTERNOON_START]
        AFTERNOON_END_X = FRAME.timeToIndex[AFTERNOON_END]

        def time_to_x(t):
            if t < MORNING_START:
                return MORNING_START_X
            elif t > AFTERNOON_END:
                return AFTERNOON_END_X
            elif MORNING_END < t < AFTERNOON_START:
                return MORNING_END
            else:
                return FRAME.timeToIndex[t]


        dic_time_to_x = {}
        dic_int_to_x = {}
        f = lambda x: str(x) if x >= 10 else '0' + str(x)
        for hour in range(24):
            for minute in range(60):
                for second in range(60):
                    s = f'{f(hour)}:{f(minute)}:{f(second)}'
                    dic_time_to_x[s] = time_to_x(s)
                    dic_int_to_x[second + minute * 100 + hour * 100_00 + 10 * 100_00_00] = dic_time_to_x[s]
        return dic_time_to_x, dic_int_to_x

    @staticmethod
    def time_int_to_x(i):
        return DIC_INT_TO_X[i]


class Plotter:
    PLOT_DIR = '/home/ubuntu/tmp/plots/'
    PLOT_NAME = 'plot_bubbles_alternative'

    @staticmethod
    def touch(subplots):
        subplots.update_layout(dragmode='pan', hovermode='closest', )
        subplots.update_xaxes(showspikes=True, spikemode='across',
                              spikesnap='cursor', spikedash='dot')
        subplots.update_yaxes(showspikes=True, spikemode='across',
                              spikesnap='cursor', spikedash='dot')
        subplots.update_layout(hoverdistance=10)
        subplots.update_xaxes(showgrid=True)
        subplots.update_layout(legend=dict(x=0, y=1, bgcolor='rgba(0,0,0,0)'))
        subplots.update_layout(margin=dict(b=20, t=25, l=0, r=0))

    @staticmethod
    def create_subplots(row_heights=None):
        from plotly.subplots import make_subplots
        if row_heights is None:
            row_heights = [0.30, 0.60]
        specs = [[{"secondary_y": True}]] * len(row_heights)

        subplots = make_subplots(
            rows=len(row_heights), cols=1,
            specs=specs,
            shared_xaxes=True,
            row_heights=row_heights,
            vertical_spacing=0.01)
        Plotter.touch(subplots)

        return subplots

    @staticmethod
    def save_and_show_plot(fig, plot_dir=None, plot_name=None, show=True):
        from dc_server.lazy_core import maybe_create_dir, execute_cmd
        from bokeh.util.browser import view
        if plot_dir is None: plot_dir = Plotter.PLOT_DIR
        if plot_name is None: plot_name = Plotter.PLOT_NAME
        maybe_create_dir(plot_dir)

        plot_name = plot_name.replace('/', '')
        fn = f'{plot_dir}/{plot_name}.html'
        fig.write_html(fn, config=dict(scrollZoom=True, ))
        # if show: view(fn)
        execute_cmd(f'ls -lahtr {fn}')

    @staticmethod
    def add_traces_to_subplot(traces, subplot, row, col, secondary_y=False):
        for trace in traces: subplot.add_trace(
            trace, row=row, col=col,
            secondary_y=secondary_y)

    @staticmethod
    def set_tick_labels(fig, dic_y_to_stock):
        for anno in fig['layout']['annotations']: anno['text'] = ''

        fig.update_xaxes(tickmode='array',
                         tickvals=TICK_VALS,
                         ticktext=TICK_TIMES)
        fig.update_yaxes(tickmode='array',
                         tickvals=list(range(len(dic_y_to_stock))),
                         ticktext=list(dic_y_to_stock.values()))

    @staticmethod
    def init():
        import numpy as np
        fig = px.imshow(np.random.random((10, 10)))
        type(fig).show = lambda x: Plotter.save_and_show_plot(x, plot_name=f'nn_fixed_{RUN_OPTION}')


class Adapters:
    @staticmethod
    def adapter_non_market_hour():
        from dc_server.ssi_ws.ssi_ws_stock_parser import BASE_PARSER
        from dc_server.lazy_core import PADAPTERS, gen_plasma_functions
        import pandas as pd
        _, _, PADAPTERS.psave1, PADAPTERS.pload1 = gen_plasma_functions(db=0)
        cube = PADAPTERS.pload1('plasma.node.js.hose500.numpy.2022_06_21')
        df = pd.DataFrame(cube, columns=BASE_PARSER.NUMERICAL_COLUMNS)
        return df

    # noinspection PyTypeChecker,PyUnresolvedReferences
    @staticmethod
    def get_tickdata_and_weights(adapter=None, df_weights: str=None):
        from dc_server.lazy_core import PADAPTERS, dt, gen_plasma_functions
        from dc_server.sotola import FRAME
        import pandas as pd
        import pickle

        ##########################################################
        # noinspection PyGlobalUndefined

        assert df_weights in [None, 'fubon', 'diamond', 'vn30'], 'Loi: unknown weight'
        if True:
            from redis import StrictRedis
            from dc_server.ssi_ws.ssi_ws_stock_parser import BASE_PARSER
            dic_stock_to_id = {v: k for k, v in BASE_PARSER.get_ssi_id_to_stock(hardcoded=True).items()}

            r = StrictRedis(WEIGHT_INPUT_REDIS_HOST)
            data = pickle.loads(r[WEIGHT_INPUT_REDIS_KEY])

            df_weights_diamond, \
            df_weights_vn30, \
            df_weights_fubon, _, _ = data


        if df_weights is None:
            df_weights = df_weights_vn30
        elif df_weights == 'fubon':
            df_weights = df_weights_fubon
        elif df_weights == 'diamond':
            df_weights = df_weights_diamond
        elif df_weights == 'vn30':
            df_weights = df_weights_vn30

        if adapter is None:
            if dt.now().strftime('%H:%M:%S') <= '09:00:00': df = Adapters.adapter_non_market_hour()
            else:
                PADAPTERS.maybe_initiate()
                df = PADAPTERS.load_ssi_ws_hose500()
        else:
            df = adapter()

        if 'id' not in df_weights: df_weights['id'] = df_weights['stock'].map(dic_stock_to_id)
        df = df[df['code'].isin(df_weights['id'])]
        cols = ['id', 'stock', 'weight', 'volume']
        df = pd.merge(df, df_weights[cols], left_on='id', right_on='id', how='left')
        ##########################################################
        """Static variables"""
        ffilter = df_weights['stock'].isin(df_weights_vn30['stock'])
        unique_stocks = df_weights.loc[~ffilter, 'stock'].tolist()
        vn30_stocks = df_weights.loc[ffilter, 'stock'].tolist()
        stocks = vn30_stocks + unique_stocks
        dic_y_to_stock = {k: v for k, v in enumerate(stocks)}
        dic_stock_to_y = {v: k for k, v in dic_y_to_stock.items()}

        return df, dic_y_to_stock, dic_stock_to_y, unique_stocks, vn30_stocks, df_weights_vn30, df_weights

    @staticmethod
    def get_ato_data():
        pass

    @staticmethod
    def create_historical_adapter(day):
        def helper():
            from dc_server.lazy_core import PADAPTERS, pickle
            from dc_server.ssi_ws.ssi_ws_base_parser import BASE_PARSER
            from redis import StrictRedis

            dic_stock_to_id = {v: k for k, v in BASE_PARSER.get_ssi_id_to_stock(hardcoded=True).items()}
            r = StrictRedis('mx')
            data = pickle.loads(r['temp.weights.fubon_diamond'])
            df_weights_diamond, \
            df_weights_vn30, \
            df_weights_fubon, _, _ = data
            PADAPTERS.maybe_initiate()

            df_weights = df_weights_vn30
            df = PICKLED_WS_ADAPTERS.load_hose500_from_db(day)['data']
            if 'id' not in df_weights: df_weights['id'] = df_weights['stock'].map(dic_stock_to_id)
            df['code'] = df['id']
            if 'stock' in df: del df['stock']
            # df = pd.merge(df, df_weights[['id', 'stock', 'weight', 'volume']], left_on='id', right_on='id', how='left')

            return df
        print('Creating historical adaper ...', end=' ')
        df = helper()
        print('Done!')
        return lambda: df


class Bubble_Domain:
    @staticmethod
    def compute_matching_volume(df, session):

        if session == 'NN':
            dic_matched_by = {'foreignerBuyVolume': 1, 'foreignerSellVolume': -1}
            dic_side = {'foreignerBuyVolume': 'BU', 'foreignerSellVolume': 'SD'}
            dic_res = {}
            for field in dic_matched_by.keys():
                dic_agg = {'time': 'first',
                           'stock': 'first',
                           field: 'first',
                           'volume': 'first',
                           'weight': 'first',
                           'session': 'first',
                           'refPrice': 'first'}
                df_grouped = df.groupby(['stock', field]).agg(dic_agg)  # .reset_index(drop=True)
                df_grouped['matchingVolume'] = df_grouped.groupby(level='stock')[field].diff(1).fillna(0).astype('int')
                del df_grouped[field]
                df_grouped['matchedBy'] = dic_matched_by[field]
                df_grouped['side'] = dic_side[field]
                dic_res[field] = df_grouped

            ##########################################################
            """Matching Volumne"""
            df_res = pd.concat(dic_res.values(), ignore_index=True)
            df_res = df_res.groupby(['matchedBy', 'stock', 'time']).agg({k: 'last' for k in df_res.columns})
            df_res = df_res[df_res['matchingVolume'] > 0].reset_index(drop=True)

            return df_res

        elif session in ['ATO', 'ATC']:

            if session == 'ATO':
                df = df[df['session']==SESSION_ATO].copy()
            else:
                df = df[df['session'].isin([SESSION_ATC, SESSION_PT])].copy()
                ##########################################################
            dic_matched_by = {'bestBid1Volume': 1, 'bestOffer1Volume': -1}
            dic_side = {'bestBid1Volume': 'BU', 'bestOffer1Volume': 'SD'}
            ##########################################################
            dic_res = {}

            """Total matched volume (fake from foreigner(Buy/Sell)Volume)"""
            for field in dic_matched_by.keys():
                field2 = field.replace('1', '2')
                price2 = field2.replace('Volume', '')

                FLOOR_CEILING = 0.065
                if field == 'bestBid1Volume':
                    ffilter = (df[price2] / df['refPrice'] >= (1 + FLOOR_CEILING)) & (df[price2] != 0)
                else:
                    ffilter = (df[price2] / df['refPrice'] <= (1 - FLOOR_CEILING)) & (df[price2] != 0)

                df.loc[ffilter, field] += df.loc[ffilter, field2]

                dic_agg = {'time': 'first',
                           'stock': 'first',
                           field: 'first',
                           'volume': 'first',
                           'session': 'first',
                           'weight': 'first',
                           'refPrice': 'first'}
                df_grouped = df.groupby(['stock', field]).agg(dic_agg)
                df_grouped['matchingVolume'] = df_grouped.groupby(level='stock')[field].diff(1).fillna(0).astype('int')
                del df_grouped[field]
                df_grouped['matchedBy'] = dic_matched_by[field]
                df_grouped['side'] = dic_side[field]
                df_grouped = df_grouped[df_grouped['matchingVolume'] > 0]
                df_grouped['volume'] = df_grouped['volume'].astype('int')

                dic_res[field] = df_grouped

            df_res = pd.concat(dic_res.values(), ignore_index=True)

            return df_res

        elif session == 'LO':
            dic_agg = {'time': 'last',
                       'stock': 'last',
                       'volume': 'last',
                       'weight': 'last',
                       'matchingVolume': 'sum',
                       'matchedBy': 'last',
                       'refPrice': 'last'}

            df_grouped = df[df['matchingVolume'] > 0].groupby(['matchedBy', 'stock', 'time']).agg(dic_agg)
            df_grouped['session'] = 'LO'
            df_grouped['side'] = df_grouped['matchedBy'].map({1: 'BU', -1: 'SD'})
            df_res = df_grouped.reset_index(drop=True)

            return df_res

        else:
            print('compute_fake_matching_volume: unknown config!')
            from time import sleep
            sleep(3)
            assert False, '\x1b[91mcompute_fake_matching_volume: unknown config!\x1b[0m'

    @staticmethod
    def compute_df_plot(df, dic_stock_to_y):
        """df_bubbles => df_plot"""
        df['x'] = df['time'].map(Utilities.time_int_to_x)
        df['t'] = df['x'].map(FRAME.indexToTime)
        df['numLots'] = (df['matchingVolume'] / df['volume']).map(lambda x: round(x, 2))
        df['y'] = df['stock'].map(dic_stock_to_y)
        df['name'] = df['stock'] + ' ' + \
                     df['t'] + '<br>' + \
                     df['matchingVolume'].map(lambda x: f'{x:,}') + \
                     df['numLots'].map(lambda x: f' ({x})')
        return df

    @staticmethod
    def compute_radius(df, MAX_NUMLOTS=300):
        import numpy as np
        return (np.log(df['numLots'].map(lambda x: min(x, MAX_NUMLOTS)) * 10 + 3) ** 2).map(lambda x: round(x, 2))

    @staticmethod
    def add_fake(df, fake_time='09:15:45'):
        if len(df) == 0: return df
        dp = df.iloc[0].to_dict()
        dp['radius'] = FAKE_RADIUS
        dp['x'] = FRAME.timeToIndex[fake_time]
        dp['side'] = 'MAX_SIZE'
        df.loc[len(df)] = dp
        return df


class Assertions:
    @staticmethod
    def assert_df_neighbour_count_shape(df_neighbour_count, df_plot):
        print(f'\x1b[96masserting that df_neighbour_count matches one-to-one vs df_plot\x1b[0m', end=' ... ')
        from time import sleep
        assert len(df_plot) == len(df_neighbour_count), print('\x1b[91m Failed! \x1b[0m', str(sleep(3)[:0]))
        print(f'\x1b[92mPassed ! [√]\x1b[0m')

    @staticmethod
    def assert_df_filtered_is_one_sided(df_filtered):
        print(f'\x1b[96masserting that df_filtred["matchedBy"] has only one unique value (either -1 or 1)\x1b[0m', end=' ... ')
        from time import sleep
        assert df_filtered['matchedBy'].nunique() == 1, print('\x1b[91m Failed! \x1b[0m', str(sleep(3)[:0]))
        print(f'\x1b[92mPassed ! [√]\x1b[0m')


class Graph:
    @staticmethod
    def sparse_to_dense(df):
        df.copy()
        indices = range(df['x'].min(), df['x'].max() + 1)
        ####### Convert from dataframe (sparse format) to array (dense format) #######

        df = df.groupby(['y', 'x']).agg({'numLots': 'last'}).reset_index()

        # df = df.reindex(indices).fillna(0)
        df = pd.pivot(df, values='numLots', index='x', columns='y')
        df_reindexed = df.reindex(indices).fillna(0)
        arr = df_reindexed.to_numpy().transpose()
        return df_reindexed, arr

    @staticmethod
    def compute_row_neighbour(right_shift, row_index):
        if right_shift > 0:
            a_shifted = np.pad(a, ((0, 0), (right_shift, 0)), mode='constant')[:, :-right_shift]
        elif right_shift < 0:
            a_shifted = np.pad(a, ((0, 0), (0, -right_shift)), mode='constant')[:, -right_shift:]
        else:
            a_shifted = a.copy()
        return np.abs(a_shifted / (a[row_index, :] + 0.00001) - 1) <= TOLERANCE

    @staticmethod
    def shift(a, right_shift):
        if right_shift > 0:
            a_shifted = np.pad(a, ((0, 0), (right_shift, 0)), mode='constant')[:, :-right_shift]
        elif right_shift < 0:
            a_shifted = np.pad(a, ((0, 0), (0, -right_shift)), mode='constant')[:, -right_shift:]
        else:
            a_shifted = a.copy()
        return a_shifted

    @staticmethod
    def compute_for_one_row_one_shift(a, row_index, right_shift):
        one_row_distance = 1 - (Graph.shift(a, right_shift) / (a[row_index, :] + 0.001))
        one_row_neighbour_matrix = abs(one_row_distance) <= TOLERANCE
        return one_row_neighbour_matrix

    @staticmethod
    def compute_for_one_row(a, row_index):
        lst = mmap(lambda shift: Graph.compute_for_one_row_one_shift(a, row_index, shift), range(-MAX_SHIFT, MAX_SHIFT + 1))
        matrix_possible_pair = sum(lst)

        return np.sum(matrix_possible_pair > 0, axis=0)


def plot(df_plot, side_unique, dic_y_to_stock, plot_name='plot', show=False, plot_title=None, color=None):
    if plot_title is None: plot_title = plot_name
    # [['x', 'y', 'radius', 'side', 'numLots', 'matchedBy', 't', 'matchingVolume', 'name', 'session']]
    if color is None: color = 'side'
    fig = px.scatter(df_plot,
                     x='x',
                     y='y',
                     size_max=SIZE_MAX,
                     category_orders={
                         'side': ['BU', 'SD', side_unique, 'MAX_SIZE', 'PT'],
                         'matchedBy': [1, -1],
                     },
                     size= 'radius',
                     render_mode='webgl',
                     color=color,
                     hover_name='name',
                     hover_data=['session'] + (['count'] if 'count' in df_plot else []),
                     facet_row='matchedBy')

    fig.update_layout(dict(title=plot_title))
    fig.update_xaxes(dict(rangeslider={'visible': True, 'thickness': 0.03}))
    fig.update_layout(yaxis=dict(side="right"), yaxis2=dict(side="right"))
    Plotter.set_tick_labels(fig, dic_y_to_stock)

    Plotter.touch(fig)

    if show:
        Plotter.save_and_show_plot(
            fig, plot_dir='/tmp/plots',
            plot_name=plot_name)
    return fig


def run(adapter=None, day=None, sessions=None, show_session_graph=False, live=False):
    from dc_server.redis_tree import REDIS_GLOBAL_TIME
    from dc_server.lazy_core import report_error, sleep
    if day is None: day = REDIS_GLOBAL_TIME.LAST_TRADING_DAY()
    if sessions is None: sessions = ALL_SESSIONS
    dic_df_plot = {}

    try:
        # for session in ['ATC']:
        for session in sessions:
            from datetime import datetime as dt
            if live and dt.now().strftime('%Y_%m_%d') <= day and '09:00:30' < dt.now().strftime('%H:%M:%S') <= '14:30:30' and session =='ATC': continue
            start_time = time()
            df_input, dic_y_to_stock, dic_stock_to_y, unique_stocks, \
            vn30_stocks, df_weights_vn30, df_weights = Adapters.get_tickdata_and_weights(adapter=adapter, df_weights=RUN_OPTION)
            df_bubbles = Bubble_Domain.compute_matching_volume(df_input, session=session)
            #############################################################################################################
            df_plot = Bubble_Domain.compute_df_plot(df_bubbles, dic_stock_to_y=dic_stock_to_y)
            side_unique = RUN_OPTION.upper() + '_UNIQUE'
            df_plot.loc[df_plot['stock'].isin(unique_stocks), 'side'] = side_unique
            df_plot['radius'] = Bubble_Domain.compute_radius(df_plot)
            fake_time = '14:29:30' if session == 'ATC' else '09:14:45'
            df_plot = Bubble_Domain.add_fake(df_plot, fake_time=fake_time)
            if session == 'ATC':
                ffilter = df_plot['session'] == SESSION_PT
                df_plot.loc[ffilter, 'side'] = 'PT'

            key = f'{RUN_OPTION}_{session}'
            dic_df_plot[key] = df_plot

            if show_session_graph:
                plot(df_plot, show=True,
                     side_unique=side_unique,
                     dic_y_to_stock=dic_y_to_stock,
                     plot_name=f'bubble_united_{RUN_OPTION.lower()}_{session}')

            print(f'Finished running {key}: {time() - start_time:,.3f} second(s)')

    except Exception as e:
        report_error(e, 'run')
        print({'day': day, 'run': RUN_OPTION, 'sess': session})
        sleep(3)

    return dic_df_plot, dic_y_to_stock


def initiate_constants():
    SESSION_ATO = 2
    SESSION_ATC = 5
    SESSION_PT = 6
    SESSION_LO = 3
    ALL_SESSIONS = ['NN', 'ATO', 'ATC', 'LO']
    return SESSION_LO, SESSION_ATC, SESSION_ATO, SESSION_PT, ALL_SESSIONS


def init_static_vars(run_option):
    import itertools

    pd.set_option('display.width', 220)
    pd.set_option('display.max_column', 200)
    pd.set_option('display.max_row', 100)
    Plotter.init()

    FAKE_RADIUS = 130
    SIZE_MAX = 60
    RUN_OPTION = ['fubon', 'diamond', 'vn30'][2] if run_option is None else run_option
    DIC_TIME_TO_X, DIC_INT_TO_X = Utilities.gen_time_to_x_lookup_tables()

    if 'tick_times' not in globals():
        TICK_TIMES = [':'.join(x) + ':00' for x in
                      itertools.product(['09', '10', '11', '12', '13', '14'], ['00', '15', '30', '45'])]
        TICK_TIMES = [x for x in TICK_TIMES if x in FRAME.timeToIndex if
                      x not in ['13:00:00', '09:00:00']]
        TICK_VALS = list(map(lambda x: FRAME.timeToIndex[x], TICK_TIMES))

    return FAKE_RADIUS, SIZE_MAX, RUN_OPTION, DIC_TIME_TO_X, DIC_INT_TO_X, TICK_TIMES, TICK_VALS


def init_static_var_graph():
    TOLERANCE = 0.30
    MAX_SHIFT = 20
    DISPLAY_THRESHOLD = 10
    return TOLERANCE, MAX_SHIFT, DISPLAY_THRESHOLD


def broad_cast_redis_weight():
    raw_data = StrictRedis(WEIGHT_INPUT_REDIS_HOST)[WEIGHT_INPUT_REDIS_KEY]

    for host in ['lv2', 'ws', 'cmc', 'cmc2', 'mx']:
        try:
            StrictRedis(host).set(WEIGHT_INPUT_REDIS_KEY, raw_data, ex=3600 * 24 * 30 * 3)
            print(f'{host} ', end='')
        except Exception as e:
            print(host, e)
    print()

########################################################################################################################
WEIGHT_INPUT_REDIS_HOST = 'cmc'
WEIGHT_INPUT_REDIS_KEY = 'temp.weights.fubon_diamond'
REDIS_PREFIX = 'bubble_united.live.{key}'
########################################################################################################################

RUN_OPTIONS = ['fubon', 'diamond', 'vn30']
run_option = RUN_OPTIONS[1]

SESSION_LO, SESSION_ATC, SESSION_ATO, SESSION_PT, ALL_SESSIONS = initiate_constants()
FAKE_RADIUS, SIZE_MAX, RUN_OPTION, DIC_TIME_TO_X, DIC_INT_TO_X, TICK_TIMES, TICK_VALS = init_static_vars(run_option)
TOLERANCE, MAX_SHIFT, DISPLAY_THRESHOLD = init_static_var_graph()

########################################################################################################################
#%
RUN_LIVE = False
def run_live():
    adapter = None
    day = None
    from redis import StrictRedis

    import pickle
    r = StrictRedis('localhost', decode_responses=False)
    iter_count = {k: 0 for k in RUN_OPTIONS}
    while True:
      for run_option in RUN_OPTIONS:
        current_time =  dt.now().strftime('%H:%M:%S')
        sessions = []
        if '09:00:30' <= current_time <= '09:16:30': sessions.append('ATO')
        if '09:16:00' <= current_time <= '14:30:30': sessions += ['NN', 'LO']
        if '14:30:15' <= current_time: sessions.append('ATC')
        if iter_count[run_option] == 0 and 'ATO' not in sessions: sessions.append('ATO')
        iter_count[run_option] += 1

        FAKE_RADIUS, SIZE_MAX, RUN_OPTION, DIC_TIME_TO_X, DIC_INT_TO_X, TICK_TIMES, TICK_VALS = init_static_vars(run_option)
        side_unique = RUN_OPTION.upper() + '_UNIQUE'
        dic_df_plot, dic_y_to_stock = run(adapter=adapter,
                                          sessions=sessions,
                                          live=True,
                                          show_session_graph=False,
                                          day=day)


        for key in dic_df_plot:
            redis_key = REDIS_PREFIX.replace('{key}', f'{key}')
            data = {'df_plot': dic_df_plot[key],
                    'time': str(dt.now()),
                    'dic_y_to_stock': dic_y_to_stock}
            r.set(redis_key, pickle.dumps(data), ex=3600*72)
            print(f'Pushed to redis: \x1b[93m{redis_key}\x1b[0m')

if __name__ == '__main__' and RUN_LIVE: run_live()


RUN_MANY = False
if __name__ == '__main__' and RUN_MANY:
    RUN_OPTIONS = ['vn30']
    sessions = ['ATO', 'NN', 'ATC']

    print('Possible Run Options: ["fubon", "vn30", "diamond"]')
    print('Possible Sessions: ["ATO", "NN", "LO", "ATC"]')
    print('possible Adapter : live (day=None), historical (day = 2022_06_xx)')

    dic_y_mapping = {}
    #for day in ['2022_06_21', '2022_06_20', '2022_06_19', '2022_06_18']:
    for day in reversed(PICKLED_WS_ADAPTERS.list_all_hose500_collections()[-5:]):
        #day = None
        if day is None:
            adapter = None
        else:
            adapter = Adapters.create_historical_adapter(day=day)
        config = 'NN'
        #######################################################  #######################################################
        assert config in ['NN', 'ATO', 'ATC', 'LO']
        dic_df_plot = {}

        t_time = time()
        for run_option in RUN_OPTIONS:
            FAKE_RADIUS, SIZE_MAX, RUN_OPTION, DIC_TIME_TO_X, DIC_INT_TO_X, TICK_TIMES, TICK_VALS = init_static_vars(run_option)
            side_unique = RUN_OPTION.upper() + '_UNIQUE'
            dic, dic_y_to_stock = run(adapter=adapter)
            dic_y_mapping[run_option] = dic_y_to_stock
            for key in dic: dic_df_plot[key] = dic[key]
        print(f'{time() - t_time:,.2f} second(s) total')
        print(dic_df_plot.keys())

        #######################################################  #######################################################
        for run_option in RUN_OPTIONS:
            lst = list(map(lambda sess: dic_df_plot[f'{run_option}_{sess}'], sessions))
            df_plot = pd.concat(lst, ignore_index=True)
            plot_name = ('live' if day is None else day) + f'_{run_option}'
            plot_title = plot_name + '_'.join([''] + sessions)
            fig = plot(df_plot, show=True,
                       dic_y_to_stock=dic_y_mapping[run_option],
                       plot_name=plot_name,
                       plot_title=plot_title,
                       side_unique=run_option.upper() + '_UNIQUE')


RUN_MANY_DAYS = True
RUN_GRAPH_ALGOS = True

RUN_OPTIONS = ['vn30', 'fubon', 'diamond']
sessions = ['ATO', 'NN', 'LO', 'ATC']
USE_NN_INSTEAD_OF_LO = False

if __name__ == '__main__':
  # for day in PICKLED_WS_ADAPTERS.list_all_hose500_collections()[-15:-10]:
  for day in PICKLED_WS_ADAPTERS.list_all_hose500_collections():
  #for day in ['2022_06_16']:
   if not RUN_MANY_DAYS: continue

   try:
    RUN_OPTION_GRAPTH_ALGOS = ['vn30', 'diamond', 'fubon'][2]

    if __name__ == '__main__' and RUN_MANY_DAYS:
        #    day = '2022_06_17'
        adapter = None if day is None else Adapters.create_historical_adapter(day)


        ####################################################
        dic_df_plot = {}
        dic_y_mapping = {}
        ####################################################

        t_time = time()
        for run_option in RUN_OPTIONS:
            FAKE_RADIUS, SIZE_MAX, RUN_OPTION, DIC_TIME_TO_X, DIC_INT_TO_X, TICK_TIMES, TICK_VALS = init_static_vars(run_option)
            side_unique = RUN_OPTION.upper() + '_UNIQUE'
            dic, dic_y_to_stock = run(adapter=adapter, sessions=sessions)
            dic_y_mapping[run_option] = dic_y_to_stock
            for key in dic: dic_df_plot[key] = dic[key]
        print(f'{time() - t_time:,.2f} second(s) total')
        #print(dic_df_plot.keys())


        for run_option in RUN_OPTIONS:
            lst = list(map(lambda sess: dic_df_plot[f'{run_option}_{sess}'], sessions))
            df_plot = pd.concat(lst, ignore_index=True)
            if RUN_OPTION_GRAPTH_ALGOS in run_option.lower():

                _df_plot = df_plot.copy()
                _dic_y_to_stock = dic_y_mapping[run_option]

            plot_name = ('live' if day is None else day) + f'_{run_option}'
            plot_title = plot_name + '_'.join([''] + sessions)
            fig = plot(df_plot, show=True,
                       dic_y_to_stock=dic_y_mapping[run_option],
                       plot_name=plot_name,
                       plot_title=plot_title,
                       side_unique=run_option.upper() + '_UNIQUE')

        print(f'\x1b[93m{list(dic_df_plot.keys())}\x1b[0m')
    #
    #%
    """"""

    if not RUN_GRAPH_ALGOS: continue
    if __name__ == '__main__' and RUN_MANY_DAYS and RUN_GRAPH_ALGOS:

      lst_sess = ['ATO', 'NN', 'ATC'] if USE_NN_INSTEAD_OF_LO else ['ATO', 'LO', 'ATC']
      df_lst = pd.DataFrame({'key': dic_df_plot.keys()})
      df_lst['run_option'] = df_lst['key'].map(lambda x: x.split('_')[0])

      for run_option in df_lst['run_option'].unique().tolist():
        _df_plot = pd.concat(list(map(lambda x: dic_df_plot[run_option + '_' + x], lst_sess)))
        _dic_y_to_stock = dic_y_mapping[run_option]
        start_time = time()
        ########################## PARAMS ############################


        ####################### RUN FOR BU ONLY #######################
        # MATCHED_BY = -1

        ######################### COMPUTATION #########################
        lst = []
        for MATCHED_BY in [-1, 1]:
            df_plot = _df_plot.copy()
            df_filtered = df_plot[(df_plot['t'] >= '09:00:00') &
                                  (df_plot['t'] <= '14:46:50') &
                                  #(df_plot['numLots'] >= 5) &
                                  #(df_plot['numLots'] <= 11) &
                                  (df_plot['matchedBy']==MATCHED_BY)]\
                .sort_values('numLots')\
                .reset_index(drop=True)

            df_reindexed, arr = Graph.sparse_to_dense(df_filtered)
            a = arr.copy()
            Assertions.assert_df_filtered_is_one_sided(df_filtered)
            bubble_strength_matrix = np.array(mmap(lambda i: Graph.compute_for_one_row(a, i), range(a.shape[0])))
            #bubble_exist_matrix = (a > 0) + 0
            df_neighbour_count = pd.DataFrame(bubble_strength_matrix,
                                              columns=df_reindexed.index,
                                              index=df_reindexed.columns).unstack().rename('count').to_frame()
            df_neighbour_count = df_neighbour_count[df_neighbour_count['count'] > 0].reset_index()


            df_plot_merged = pd.merge(df_neighbour_count.groupby(['x', 'y']).last(), df_filtered.groupby(['x', 'y']).last(), left_index=True, right_index=True, how='right').reset_index()

            #####
            Bubble_Domain.add_fake(df_plot_merged)
            ffilter = df_plot_merged['side'] == 'MAX_SIZE'
            df_plot_merged.loc[ffilter, 'count'] = DISPLAY_THRESHOLD
            df_plot_merged.loc[ffilter, 'x'] = FRAME.timeToIndex['14:45:50']
            lst.append(df_plot_merged)
        #%
        #####
        df_plot_with_neighbour = pd.concat(lst, ignore_index=True)
        maybe_create_dir(f'/tmp/data/')
        with open(f'/tmp/data/{day}_{run_option}.pickle', 'wb') as file:
            pickle.dump(df_plot_with_neighbour, file)
        print('\x1b[94m' + '=' * 90 + '\x1b[0m')
        print('Dumped df_plot_with_neighbour to \x1b[94m' + f'/tmp/data/s{day}_{run_option}.pickle' + '\x1b[0m')
        df_plot_with_neighbour['type'] = df_plot_with_neighbour['matchedBy'].map({1: 'Arbit', -1: 'Unwind'})
        df_plot_with_neighbour = df_plot_with_neighbour[df_plot_with_neighbour['count'] >= DISPLAY_THRESHOLD]


        print(f'Plotting bubbles with min_strength=\x1b[93m{DISPLAY_THRESHOLD}\x1b[0m & shift=\x1b[93m{MAX_SHIFT}\x1b[0m')
        fig = px.scatter(df_plot_with_neighbour,
                         x='x', y='y',
                         hover_name='name',
                         color='count',
                         render_mode='webgl',
                         facet_row='type',
                         hover_data=['numLots'],
                         category_orders={'type': ['Arbit', 'Unwind']},
                         size_max=60,
                         size='radius')
        Plotter.touch(fig)
        Plotter.set_tick_labels(fig, _dic_y_to_stock)

        fig.update_layout(legend=dict(x=0, y=1, bgcolor='rgba(255,255,0,0)'))
        fig.update_xaxes(dict(rangeslider={'visible': True, 'thickness': 0.03}))
        fig.update_layout(yaxis=dict(side="right"), yaxis2=dict(side="right"))


        fn = f'/tmp/plots/{day}_{run_option}_with_counts.html'
        maybe_create_dir('/tmp/plots/')
        fig.write_html(fn, config={
            'scrollZoom': True
        })
        # view(fn)
        print(f'{time() - start_time:,.2f} second(s)')

   except Exception as e:
    from dc_server.lazy_core import report_error, sleep
    report_error(e)
    sleep(3)

