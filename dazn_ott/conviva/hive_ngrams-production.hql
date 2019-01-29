SELECT asset,deviceos FROM  staged_akamai.conviva_logs where country = '"japan"' limit 10 ;
SELECT explode(ngrams(sentences(lower(asset)), 8, 30)) AS top30of8 FROM  staged_akamai.conviva_logs where country = '"japan"' limit 30 ;
SELECT asset,deviceos FROM  staged_akamai.conviva_logs where country <> '"japan"' limit 10 ;
SELECT explode(ngrams(sentences(lower(asset)), 5, 30)) AS top30of5 FROM  staged_akamai.conviva_logs  where country <> '"japan"' limit 30 ;
