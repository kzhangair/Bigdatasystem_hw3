N = 100;
M = randn(N);
columMin = min(M, [], 1);
columMax = max(M, [], 1);
min = repmat(columMin, N,1);
max = repmat(columMax, N,1);
positiveM = M-min;
sumColumn = sum(positiveM,1);
sumColumn = repmat(sumColumn, N,1);
normalizedM = positiveM./sumColumn;
normalizedM = round(normalizedM * 1e4)/1e4;
normalizedM =normalizedM';
V= repmat(1.0/N, N, 1);
enM = [normalizedM V];