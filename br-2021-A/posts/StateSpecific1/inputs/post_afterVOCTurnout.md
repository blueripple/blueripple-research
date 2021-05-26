Across the states, gaps vary by 32 points, from a high of +4%
in AL to -28% in WA.
Can this gap be explained by demographic differences alone? For example,
the VOC in AL are mostly Black while the VOC in WA are mostly Hispanic.  Black
voters turned out at higher rates than Hispanic voters nationwide.  Perhaps
this explains the between WA and PA? Using our model, we can
estimate the gaps that would result if there were no state-specific effects[^3].
We chart this below.

[^3]: We post-stratify with and without the interaction term.  We *should* model
    the two situations separately: the presence of the interaction term
    in the model shifts the other parameters. But this introduces
    complications when trying to estimate confidence intervals for various quantities.
    For more details, [click here.][niComparison_link]
