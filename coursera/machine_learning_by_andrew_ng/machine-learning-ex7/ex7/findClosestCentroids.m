function idx = findClosestCentroids(X, centroids)
%FINDCLOSESTCENTROIDS computes the centroid memberships for every example
%   idx = FINDCLOSESTCENTROIDS (X, centroids) returns the closest centroids
%   in idx for a dataset X where each row is a single example. idx = m x 1 
%   vector of centroid assignments (i.e. each entry in range [1..K])
%

% Set K
K = size(centroids, 1);

% You need to return the following variables correctly.
idx = zeros(size(X,1), 1);

% ====================== YOUR CODE HERE ======================
% Instructions: Go over every example, find its closest centroid, and store
%               the index inside idx at the appropriate location.
%               Concretely, idx(i) should contain the index of the centroid
%               closest to example i. Hence, it should be a value in the 
%               range 1..K
%
% Note: You can use a for-loop over the examples to compute this.
%
for m=1:length(X)
    % 1 to length of X
	distances = zeros(size(centroids, 1), 1);
    
    % 1 to K(number of segments)
	for k=1:K
        % location
        location = X(m, :);
        centroid = centroids(k, :);
        
        % get distnace x from centroid of K 
		distances(k) = sum((location - centroid) .^ 2 );
	end
	[~, index] = min(distances);
    
    % set segment
	idx(m) = index;
end







% =============================================================

end

