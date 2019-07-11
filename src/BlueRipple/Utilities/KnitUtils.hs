{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}
module BlueRipple.Utilities.KnitUtils where

import qualified Knit.Report                   as K
import qualified Control.Monad.Except          as X
import qualified Data.Text                     as T

import           Polysemy.Error                 ( Error )

knitX
  :: forall r a
   . K.Member (Error K.PandocError) r
  => X.ExceptT T.Text (K.Sem r) a
  -> K.Sem r a
knitX ma = X.runExceptT ma >>= (knitEither @r)

knitMaybe
  :: forall r a
   . K.Member (Error K.PandocError) r
  => T.Text
  -> Maybe a
  -> K.Sem r a
knitMaybe msg ma = maybe (K.knitError msg) return ma

knitEither
  :: forall r a
   . K.Member (Error K.PandocError) r
  => Either T.Text a
  -> K.Sem r a
knitEither = either K.knitError return
