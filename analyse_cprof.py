import pstats
p = pstats.Stats('cprofile.out')

p.sort_stats('cumulative').print_stats()