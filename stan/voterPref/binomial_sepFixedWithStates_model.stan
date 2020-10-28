data {
int<lower = 0> G; // number of cells
  int<lower = 1> J_state; // number of states
  int<lower = 1, upper = J_state> state[G];
  int<lower = 1> K; // number of cols in predictor matrix
  matrix[G, K] X;
  int<lower = 0> D_votes[G];
  int<lower = 0> Total_votes[G];
  int<lower = 0> M;
  int<lower = 0> predict_State[M];
  matrix[M, K] predict_X;
}
parameters {
real alpha;
  vector[K] beta;
  real<lower=0> sigma_aState;
  vector<multiplier=sigma_aState> [J_state] aState;
}
model {
alpha ~ normal(0,2);
  beta ~ normal(0,1);
  sigma_aState ~ normal(0, 10);
  aState ~ normal(0, sigma_aState);
  D_votes ~ binomial_logit(Total_votes, alpha + (X * beta) + aState[state]);
}
generated quantities {
vector<lower = 0, upper = 1>[M] predicted;
  predicted = inv_logit(alpha + (predict_X * beta) + aState[predict_State]);
}
