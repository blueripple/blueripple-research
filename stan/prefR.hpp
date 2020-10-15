
// Code generated by stanc v2.24.1
#include <stan/model/model_header.hpp>
namespace prefR_model_namespace {


inline void validate_positive_index(const char* var_name, const char* expr,
                                    int val) {
  if (val < 1) {
    std::stringstream msg;
    msg << "Found dimension size less than one in simplex declaration"
        << "; variable=" << var_name << "; dimension size expression=" << expr
        << "; expression value=" << val;
    std::string msg_str(msg.str());
    throw std::invalid_argument(msg_str.c_str());
  }
}

inline void validate_unit_vector_index(const char* var_name, const char* expr,
                                       int val) {
  if (val <= 1) {
    std::stringstream msg;
    if (val == 1) {
      msg << "Found dimension size one in unit vector declaration."
          << " One-dimensional unit vector is discrete"
          << " but the target distribution must be continuous."
          << " variable=" << var_name << "; dimension size expression=" << expr;
    } else {
      msg << "Found dimension size less than one in unit vector declaration"
          << "; variable=" << var_name << "; dimension size expression=" << expr
          << "; expression value=" << val;
    }
    std::string msg_str(msg.str());
    throw std::invalid_argument(msg_str.c_str());
  }
}


using std::istream;
using std::string;
using std::stringstream;
using std::vector;
using std::pow;
using stan::io::dump;
using stan::math::lgamma;
using stan::model::model_base_crtp;
using stan::model::rvalue;
using stan::model::cons_list;
using stan::model::index_uni;
using stan::model::index_max;
using stan::model::index_min;
using stan::model::index_min_max;
using stan::model::index_multi;
using stan::model::index_omni;
using stan::model::nil_index_list;
using namespace stan::math;
using stan::math::pow; 

static int current_statement__ = 0;
static const std::vector<string> locations_array__ = {" (found before start of program)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 20, column 2 to column 47)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 42, column 2 to column 71)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 43, column 2 to column 36)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 37, column 2 to column 56)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 2, column 2 to column 19)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 3, column 2 to column 25)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 4, column 2 to column 23)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 5, column 2 to column 23)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 6, column 2 to column 24)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 7, column 2 to column 24)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 13, column 67 to column 68)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 13, column 2 to column 70)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 14, column 25 to column 26)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 14, column 2 to column 28)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 15, column 29 to column 30)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 15, column 2 to column 32)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 20, column 9 to column 40)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/prefR.stan', line 42, column 32 to column 63)"};



class prefR_model final : public model_base_crtp<prefR_model> {

 private:
  int G;
  int J_state;
  int J_sex;
  int J_age;
  int J_educ;
  int J_race;
  std::vector<int> category;
  std::vector<int> D_votes;
  std::vector<int> Total_votes;
  int beta_1dim__;
  int probs_1dim__;
 
 public:
  ~prefR_model() final { }
  
  std::string model_name() const final { return "prefR_model"; }

  std::vector<std::string> model_compile_info() const {
    std::vector<std::string> stanc_info;
    stanc_info.push_back("stanc_version = stanc3 v2.24.1");
    stanc_info.push_back("stancflags = --include-paths= --use-opencl");
    return stanc_info;
  }
  
  
  prefR_model(stan::io::var_context& context__,
              unsigned int random_seed__ = 0,
              std::ostream* pstream__ = nullptr) : model_base_crtp(0) {
    using local_scalar_t__ = double ;
    boost::ecuyer1988 base_rng__ = 
        stan::services::util::create_rng(random_seed__, 0);
    (void) base_rng__;  // suppress unused var warning
    static const char* function__ = "prefR_model_namespace::prefR_model";
    (void) function__;  // suppress unused var warning
    local_scalar_t__ DUMMY_VAR__(std::numeric_limits<double>::quiet_NaN());
    (void) DUMMY_VAR__;  // suppress unused var warning
    
    try {
      int pos__;
      pos__ = std::numeric_limits<int>::min();
      
      pos__ = 1;
      current_statement__ = 5;
      context__.validate_dims("data initialization","G","int",
          context__.to_vec());
      G = std::numeric_limits<int>::min();
      
      current_statement__ = 5;
      G = context__.vals_i("G")[(1 - 1)];
      current_statement__ = 5;
      current_statement__ = 5;
      check_greater_or_equal(function__, "G", G, 0);
      current_statement__ = 6;
      context__.validate_dims("data initialization","J_state","int",
          context__.to_vec());
      J_state = std::numeric_limits<int>::min();
      
      current_statement__ = 6;
      J_state = context__.vals_i("J_state")[(1 - 1)];
      current_statement__ = 6;
      current_statement__ = 6;
      check_greater_or_equal(function__, "J_state", J_state, 1);
      current_statement__ = 7;
      context__.validate_dims("data initialization","J_sex","int",
          context__.to_vec());
      J_sex = std::numeric_limits<int>::min();
      
      current_statement__ = 7;
      J_sex = context__.vals_i("J_sex")[(1 - 1)];
      current_statement__ = 7;
      current_statement__ = 7;
      check_greater_or_equal(function__, "J_sex", J_sex, 1);
      current_statement__ = 8;
      context__.validate_dims("data initialization","J_age","int",
          context__.to_vec());
      J_age = std::numeric_limits<int>::min();
      
      current_statement__ = 8;
      J_age = context__.vals_i("J_age")[(1 - 1)];
      current_statement__ = 8;
      current_statement__ = 8;
      check_greater_or_equal(function__, "J_age", J_age, 1);
      current_statement__ = 9;
      context__.validate_dims("data initialization","J_educ","int",
          context__.to_vec());
      J_educ = std::numeric_limits<int>::min();
      
      current_statement__ = 9;
      J_educ = context__.vals_i("J_educ")[(1 - 1)];
      current_statement__ = 9;
      current_statement__ = 9;
      check_greater_or_equal(function__, "J_educ", J_educ, 1);
      current_statement__ = 10;
      context__.validate_dims("data initialization","J_race","int",
          context__.to_vec());
      J_race = std::numeric_limits<int>::min();
      
      current_statement__ = 10;
      J_race = context__.vals_i("J_race")[(1 - 1)];
      current_statement__ = 10;
      current_statement__ = 10;
      check_greater_or_equal(function__, "J_race", J_race, 1);
      current_statement__ = 11;
      validate_non_negative_index("category", "G", G);
      current_statement__ = 12;
      context__.validate_dims("data initialization","category","int",
          context__.to_vec(G));
      category = std::vector<int>(G, std::numeric_limits<int>::min());
      
      current_statement__ = 12;
      assign(category, nil_index_list(), context__.vals_i("category"),
        "assigning variable category");
      current_statement__ = 12;
      for (int sym1__ = 1; sym1__ <= G; ++sym1__) {
        current_statement__ = 12;
        current_statement__ = 12;
        check_greater_or_equal(function__, "category[sym1__]",
                               category[(sym1__ - 1)], 1);}
      current_statement__ = 12;
      for (int sym1__ = 1; sym1__ <= G; ++sym1__) {
        current_statement__ = 12;
        current_statement__ = 12;
        check_less_or_equal(function__, "category[sym1__]",
                            category[(sym1__ - 1)],
                            (((J_age * J_sex) * J_educ) * J_race));}
      current_statement__ = 13;
      validate_non_negative_index("D_votes", "G", G);
      current_statement__ = 14;
      context__.validate_dims("data initialization","D_votes","int",
          context__.to_vec(G));
      D_votes = std::vector<int>(G, std::numeric_limits<int>::min());
      
      current_statement__ = 14;
      assign(D_votes, nil_index_list(), context__.vals_i("D_votes"),
        "assigning variable D_votes");
      current_statement__ = 14;
      for (int sym1__ = 1; sym1__ <= G; ++sym1__) {
        current_statement__ = 14;
        current_statement__ = 14;
        check_greater_or_equal(function__, "D_votes[sym1__]",
                               D_votes[(sym1__ - 1)], 0);}
      current_statement__ = 15;
      validate_non_negative_index("Total_votes", "G", G);
      current_statement__ = 16;
      context__.validate_dims("data initialization","Total_votes","int",
          context__.to_vec(G));
      Total_votes = std::vector<int>(G, std::numeric_limits<int>::min());
      
      current_statement__ = 16;
      assign(Total_votes, nil_index_list(), context__.vals_i("Total_votes"),
        "assigning variable Total_votes");
      current_statement__ = 16;
      for (int sym1__ = 1; sym1__ <= G; ++sym1__) {
        current_statement__ = 16;
        current_statement__ = 16;
        check_greater_or_equal(function__, "Total_votes[sym1__]",
                               Total_votes[(sym1__ - 1)], 0);}
      current_statement__ = 17;
      beta_1dim__ = std::numeric_limits<int>::min();
      
      current_statement__ = 17;
      beta_1dim__ = (((J_sex * J_age) * J_educ) * J_race);
      current_statement__ = 17;
      validate_non_negative_index("beta", "J_sex * J_age * J_educ * J_race",
                                  beta_1dim__);
      current_statement__ = 18;
      probs_1dim__ = std::numeric_limits<int>::min();
      
      current_statement__ = 18;
      probs_1dim__ = (((J_age * J_sex) * J_educ) * J_race);
      current_statement__ = 18;
      validate_non_negative_index("probs", "J_age * J_sex * J_educ * J_race",
                                  probs_1dim__);
    } catch (const std::exception& e) {
      stan::lang::rethrow_located(e, locations_array__[current_statement__]);
      // Next line prevents compiler griping about no return
      throw std::runtime_error("*** IF YOU SEE THIS, PLEASE REPORT A BUG ***"); 
    }
    num_params_r__ = 0U;
    
    try {
      num_params_r__ += beta_1dim__;
    } catch (const std::exception& e) {
      stan::lang::rethrow_located(e, locations_array__[current_statement__]);
      // Next line prevents compiler griping about no return
      throw std::runtime_error("*** IF YOU SEE THIS, PLEASE REPORT A BUG ***"); 
    }
  }
  template <bool propto__, bool jacobian__, typename T__>
  inline T__ log_prob(std::vector<T__>& params_r__,
                      std::vector<int>& params_i__,
                      std::ostream* pstream__ = nullptr) const {
    using local_scalar_t__ = T__;
    T__ lp__(0.0);
    stan::math::accumulator<T__> lp_accum__;
    static const char* function__ = "prefR_model_namespace::log_prob";
(void) function__;  // suppress unused var warning

    stan::io::reader<local_scalar_t__> in__(params_r__, params_i__);
    local_scalar_t__ DUMMY_VAR__(std::numeric_limits<double>::quiet_NaN());
    (void) DUMMY_VAR__;  // suppress unused var warning

    
    try {
      Eigen::Matrix<local_scalar_t__, -1, 1> beta;
      beta = Eigen::Matrix<local_scalar_t__, -1, 1>(beta_1dim__);
      stan::math::fill(beta, DUMMY_VAR__);
      
      current_statement__ = 1;
      beta = in__.vector(beta_1dim__);
      {
        current_statement__ = 4;
        lp_accum__.add(
          binomial_logit_lpmf<propto__>(D_votes, Total_votes,
            rvalue(beta, cons_list(index_multi(category), nil_index_list()),
              "beta")));
      }
    } catch (const std::exception& e) {
      stan::lang::rethrow_located(e, locations_array__[current_statement__]);
      // Next line prevents compiler griping about no return
      throw std::runtime_error("*** IF YOU SEE THIS, PLEASE REPORT A BUG ***"); 
    }
    lp_accum__.add(lp__);
    return lp_accum__.sum();
    } // log_prob() 
    
  template <typename RNG>
  inline void write_array(RNG& base_rng__, std::vector<double>& params_r__,
                          std::vector<int>& params_i__,
                          std::vector<double>& vars__,
                          bool emit_transformed_parameters__ = true,
                          bool emit_generated_quantities__ = true,
                          std::ostream* pstream__ = nullptr) const {
    using local_scalar_t__ = double;
    vars__.resize(0);
    stan::io::reader<local_scalar_t__> in__(params_r__, params_i__);
    static const char* function__ = "prefR_model_namespace::write_array";
(void) function__;  // suppress unused var warning

    (void) function__;  // suppress unused var warning

    double lp__ = 0.0;
    (void) lp__;  // dummy to suppress unused var warning
    stan::math::accumulator<double> lp_accum__;
    local_scalar_t__ DUMMY_VAR__(std::numeric_limits<double>::quiet_NaN());
    (void) DUMMY_VAR__;  // suppress unused var warning

    
    try {
      Eigen::Matrix<double, -1, 1> beta;
      beta = Eigen::Matrix<double, -1, 1>(beta_1dim__);
      stan::math::fill(beta, std::numeric_limits<double>::quiet_NaN());
      
      current_statement__ = 1;
      beta = in__.vector(beta_1dim__);
      for (int sym1__ = 1; sym1__ <= beta_1dim__; ++sym1__) {
        vars__.emplace_back(beta[(sym1__ - 1)]);}
      if (logical_negation((primitive_value(emit_transformed_parameters__) ||
            primitive_value(emit_generated_quantities__)))) {
        return ;
      } 
      if (logical_negation(emit_generated_quantities__)) {
        return ;
      } 
      Eigen::Matrix<double, -1, 1> probs;
      probs = Eigen::Matrix<double, -1, 1>(probs_1dim__);
      stan::math::fill(probs, std::numeric_limits<double>::quiet_NaN());
      
      current_statement__ = 3;
      assign(probs, nil_index_list(),
        inv_logit(
          rvalue(beta, cons_list(index_multi(category), nil_index_list()),
            "beta")), "assigning variable probs");
      current_statement__ = 2;
      for (int sym1__ = 1; sym1__ <= probs_1dim__; ++sym1__) {
        current_statement__ = 2;
        current_statement__ = 2;
        check_greater_or_equal(function__, "probs[sym1__]",
                               probs[(sym1__ - 1)], 0);}
      current_statement__ = 2;
      for (int sym1__ = 1; sym1__ <= probs_1dim__; ++sym1__) {
        current_statement__ = 2;
        current_statement__ = 2;
        check_less_or_equal(function__, "probs[sym1__]", probs[(sym1__ - 1)],
                            1);}
      for (int sym1__ = 1; sym1__ <= probs_1dim__; ++sym1__) {
        vars__.emplace_back(probs[(sym1__ - 1)]);}
    } catch (const std::exception& e) {
      stan::lang::rethrow_located(e, locations_array__[current_statement__]);
      // Next line prevents compiler griping about no return
      throw std::runtime_error("*** IF YOU SEE THIS, PLEASE REPORT A BUG ***"); 
    }
    } // write_array() 
    
  inline void transform_inits(const stan::io::var_context& context__,
                              std::vector<int>& params_i__,
                              std::vector<double>& vars__,
                              std::ostream* pstream__) const
    final {
    using local_scalar_t__ = double;
    vars__.clear();
    vars__.reserve(num_params_r__);
    
    try {
      int pos__;
      pos__ = std::numeric_limits<int>::min();
      
      pos__ = 1;
      Eigen::Matrix<double, -1, 1> beta;
      beta = Eigen::Matrix<double, -1, 1>(beta_1dim__);
      stan::math::fill(beta, std::numeric_limits<double>::quiet_NaN());
      
      {
        std::vector<local_scalar_t__> beta_flat__;
        current_statement__ = 1;
        assign(beta_flat__, nil_index_list(), context__.vals_r("beta"),
          "assigning variable beta_flat__");
        current_statement__ = 1;
        pos__ = 1;
        current_statement__ = 1;
        for (int sym1__ = 1; sym1__ <= beta_1dim__; ++sym1__) {
          current_statement__ = 1;
          assign(beta, cons_list(index_uni(sym1__), nil_index_list()),
            beta_flat__[(pos__ - 1)], "assigning variable beta");
          current_statement__ = 1;
          pos__ = (pos__ + 1);}
      }
      for (int sym1__ = 1; sym1__ <= beta_1dim__; ++sym1__) {
        vars__.emplace_back(beta[(sym1__ - 1)]);}
    } catch (const std::exception& e) {
      stan::lang::rethrow_located(e, locations_array__[current_statement__]);
      // Next line prevents compiler griping about no return
      throw std::runtime_error("*** IF YOU SEE THIS, PLEASE REPORT A BUG ***"); 
    }
    } // transform_inits() 
    
  inline void get_param_names(std::vector<std::string>& names__) const {
    
    names__.clear();
    names__.emplace_back("beta");
    names__.emplace_back("probs");
    } // get_param_names() 
    
  inline void get_dims(std::vector<std::vector<size_t>>& dimss__) const
    final {
    dimss__.clear();
    dimss__.emplace_back(std::vector<size_t>{static_cast<size_t>(beta_1dim__)});
    
    dimss__.emplace_back(std::vector<size_t>{
                                             static_cast<size_t>(probs_1dim__)});
    
    } // get_dims() 
    
  inline void constrained_param_names(
                                      std::vector<std::string>& param_names__,
                                      bool emit_transformed_parameters__ = true,
                                      bool emit_generated_quantities__ = true) const
    final {
    
    for (int sym1__ = 1; sym1__ <= beta_1dim__; ++sym1__) {
      {
        param_names__.emplace_back(std::string() + "beta" + '.' + std::to_string(sym1__));
      }}
    if (emit_transformed_parameters__) {
      
    }
    
    if (emit_generated_quantities__) {
      for (int sym1__ = 1; sym1__ <= probs_1dim__; ++sym1__) {
        {
          param_names__.emplace_back(std::string() + "probs" + '.' + std::to_string(sym1__));
        }}
    }
    
    } // constrained_param_names() 
    
  inline void unconstrained_param_names(
                                        std::vector<std::string>& param_names__,
                                        bool emit_transformed_parameters__ = true,
                                        bool emit_generated_quantities__ = true) const
    final {
    
    for (int sym1__ = 1; sym1__ <= beta_1dim__; ++sym1__) {
      {
        param_names__.emplace_back(std::string() + "beta" + '.' + std::to_string(sym1__));
      }}
    if (emit_transformed_parameters__) {
      
    }
    
    if (emit_generated_quantities__) {
      for (int sym1__ = 1; sym1__ <= probs_1dim__; ++sym1__) {
        {
          param_names__.emplace_back(std::string() + "probs" + '.' + std::to_string(sym1__));
        }}
    }
    
    } // unconstrained_param_names() 
    
  inline std::string get_constrained_sizedtypes() const {
    stringstream s__;
    s__ << "[{\"name\":\"beta\",\"type\":{\"name\":\"vector\",\"length\":" << beta_1dim__ << "},\"block\":\"parameters\"},{\"name\":\"probs\",\"type\":{\"name\":\"vector\",\"length\":" << probs_1dim__ << "},\"block\":\"generated_quantities\"}]";
    return s__.str();
    } // get_constrained_sizedtypes() 
    
  inline std::string get_unconstrained_sizedtypes() const {
    stringstream s__;
    s__ << "[{\"name\":\"beta\",\"type\":{\"name\":\"vector\",\"length\":" << beta_1dim__ << "},\"block\":\"parameters\"},{\"name\":\"probs\",\"type\":{\"name\":\"vector\",\"length\":" << probs_1dim__ << "},\"block\":\"generated_quantities\"}]";
    return s__.str();
    } // get_unconstrained_sizedtypes() 
    
  
    // Begin method overload boilerplate
    template <typename RNG>
    inline void write_array(RNG& base_rng__,
                     Eigen::Matrix<double,Eigen::Dynamic,1>& params_r,
                     Eigen::Matrix<double,Eigen::Dynamic,1>& vars,
                     bool emit_transformed_parameters__ = true,
                     bool emit_generated_quantities__ = true,
                     std::ostream* pstream = nullptr) const {
      std::vector<double> params_r_vec(params_r.size());
      for (int i = 0; i < params_r.size(); ++i)
        params_r_vec[i] = params_r(i);
      std::vector<double> vars_vec;
      std::vector<int> params_i_vec;
      write_array(base_rng__, params_r_vec, params_i_vec, vars_vec,
          emit_transformed_parameters__, emit_generated_quantities__, pstream);
      vars.resize(vars_vec.size());
      for (int i = 0; i < vars.size(); ++i)
        vars(i) = vars_vec[i];
    }

    template <bool propto__, bool jacobian__, typename T_>
    inline T_ log_prob(Eigen::Matrix<T_,Eigen::Dynamic,1>& params_r,
               std::ostream* pstream = nullptr) const {
      std::vector<T_> vec_params_r;
      vec_params_r.reserve(params_r.size());
      for (int i = 0; i < params_r.size(); ++i)
        vec_params_r.push_back(params_r(i));
      std::vector<int> vec_params_i;
      return log_prob<propto__,jacobian__,T_>(vec_params_r, vec_params_i, pstream);
    }

    inline void transform_inits(const stan::io::var_context& context,
                         Eigen::Matrix<double, Eigen::Dynamic, 1>& params_r,
                         std::ostream* pstream__ = nullptr) const {
      std::vector<double> params_r_vec;
      std::vector<int> params_i_vec;
      transform_inits(context, params_i_vec, params_r_vec, pstream__);
      params_r.resize(params_r_vec.size());
      for (int i = 0; i < params_r.size(); ++i)
        params_r(i) = params_r_vec[i];
    }

};
}
using stan_model = prefR_model_namespace::prefR_model;

#ifndef USING_R

// Boilerplate
stan::model::model_base& new_model(
        stan::io::var_context& data_context,
        unsigned int seed,
        std::ostream* msg_stream) {
  stan_model* m = new stan_model(data_context, seed, msg_stream);
  return *m;
}

#endif


