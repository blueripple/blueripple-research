{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}

module Stan.ModelBuilder.TypedExpressions.Program
  (
    module Stan.ModelBuilder.TypedExpressions.Program
  )
  where

import qualified Stan.ModelBuilder.TypedExpressions.Statements as TB
import qualified Stan.ModelBuilder.BuilderTypes as SBT
import Relude.Extra
import qualified Data.Array as Array

newtype StanProgram = StanProgram { unStanProgram :: Array.Array SBT.StanBlock [TB.UStmt] }

programToStmt :: StanProgram -> TB.UStmt
programToStmt = undefined
