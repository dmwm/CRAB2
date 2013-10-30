#!/usr/bin/env python
import profile

###########################################################################
if __name__ == '__main__':
    
    import pstats
    p = pstats.Stats('crab.profile')

    p.strip_dirs().sort_stats(-1).print_stats()
    p.sort_stats('name')
    p.print_stats()

    p.sort_stats('cumulative').print_stats(50)

    # p.sort_stats('file').print_stats('__init__')

    # p.sort_stats('time', 'cum').print_stats(.5, 'init')

    # p.print_callers(.5, 'init')

    # p.print_callees()
    #p.add('fooprof')



