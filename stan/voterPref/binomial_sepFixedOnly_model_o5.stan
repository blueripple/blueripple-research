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
  matrix[M, K] predict_X[M];
}
parameters {
real alpha;
  vector[K] beta;
}
model {
D_votes ~ binomial_logit(Total_votes, alpha + X * beta);
}
generated quantities {
vector<lower = 0, upper = 1>[M] predicted;
  for (p in 1:M) {
    real xBeta;
    for (k in 1:K) {
      xBeta = X[p, k] * beta[k];
    }
    predicted[p] = inv_logit(alpha + xBeta);
  }
}
